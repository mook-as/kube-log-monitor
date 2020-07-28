package kubelogmonitor

import (
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	apicorev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

// logRequests is the map of pod name -> container id -> log stream.
// It must only be accessed under logRequestLock.
// If the entry exists and is non-nil, we are actively watching it.
// If the entry exists and is nil, the pod had terminated (and the logs are done).
var logRequests map[string]map[string]io.ReadCloser

// logRequestLock is the lock for logRequests
var logRequestLock sync.Mutex

const maxStatusMessage = 20

// message, namespace, pod, container string, error error
var statusFunc func(message string, pieces ...interface{})

func statusVerbose(message string, pieces ...interface{}) {
	if len(message) > maxStatusMessage {
		panic(fmt.Sprintf("Invalid message: %q", message))
	}
	args := []interface{}{maxStatusMessage, message}
	formats := []string{"%-*s"}
	extra_formats := []string{": %s", "/%s", ":%s"}
	for i, arg := range pieces {
		if _, ok := arg.(error); ok {
			break
		}
		if i < len(extra_formats) {
			formats = append(formats, extra_formats[i])
		} else {
			formats = append(formats, " %s")
		}
		args = append(args, arg)
	}
	if len(pieces) > 0 {
		if err, ok := pieces[len(pieces)-1].(error); ok {
			formats = append(formats, " error=%v")
			args = append(args, err)
		}
	}
	fmt.Printf(strings.Join(formats, "")+"\n", args...)
}
func statusSilent(message string, pieces ...interface{}) {
	if len(message) > maxStatusMessage {
		panic(fmt.Sprintf("Invalid message: %q", message))
	}
}

func Monitor(client *kubernetes.Clientset, outputDir string, namespaces []string, podMatcher, containerMatcher *regexp.Regexp) error {
	logRequestLock.Lock()
	if logRequests == nil {
		logRequests = make(map[string]map[string]io.ReadCloser)
	}
	if outputDir != "" {
		statusFunc = statusVerbose
	} else {
		statusFunc = statusSilent
	}
	logRequestLock.Unlock()

	errors := make(chan error)
	for _, namespace := range namespaces {
		go func(namespace string) {
			namespaceChannel, err := monitorNamespace(client, namespace)
			if err != nil {
				errors <- fmt.Errorf("could not monitor namespace %s: %w", namespace, err)
				return
			}
			for {
				podsChannel := monitorPods(client, namespace, namespaceChannel)
				podLister := client.CoreV1().Pods(namespace)
				statusFunc("Monitoring namespace", namespace)
				for podEvent := range podsChannel {
					if podEvent.err != nil {
						errors <- podEvent.err
						return
					}
					if !podMatcher.MatchString(podEvent.podName) {
						statusFunc("Skipping pod", namespace, podEvent.podName)
						continue
					}
					for i := 0; i < 3; i++ {
						if err = watchPod(outputDir, namespace, podLister, podEvent, containerMatcher); err != nil {
							break
						}
					}
					if err != nil {
						errors <- err
						return
					}
				}
			}
		}(namespace)
	}

	return <-errors
}

// monitorNamesapce returns a channel that generates events when the target
// namespace has been created or deleted
func monitorNamespace(client *kubernetes.Clientset, namespace string) (<-chan struct{}, error) {
	ns := client.CoreV1().Namespaces()
	namespaces, err := ns.List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	watcher, err := ns.Watch(metav1.ListOptions{
		Watch:           true,
		ResourceVersion: namespaces.GetResourceVersion(),
	})
	if err != nil {
		return nil, err
	}
	result := make(chan struct{})
	go func() {
		defer watcher.Stop()
		for event := range watcher.ResultChan() {
			switch event.Type {
			case watch.Modified:
				// Do nothing
			case watch.Added, watch.Deleted:
				changedNamespace, ok := event.Object.(*apicorev1.Namespace)
				if !ok {
					fmt.Printf("unexpected non-namespace object deleted: %s", event.Object.DeepCopyObject().GetObjectKind())
				} else if changedNamespace.Name == namespace {
					result <- struct{}{}
				}
			default:
				fmt.Printf("unexpected namespace event type %s", event.Type)
			}
		}
	}()
	return result, nil
}

type containerState string

const (
	containerStateRunning    = containerState("running")
	containerStateTerminated = containerState("terminated")
	containerStateWaiting    = containerState("waiting")
	containerStateOther      = containerState("unknown")
)

type containerInfo struct {
	name  string
	id    string
	state containerState
}

func (c *containerInfo) String() string {
	return fmt.Sprintf("%s-%s", c.name, c.id)
}

type podInfoOrError struct {
	err        error
	podName    string
	containers []containerInfo
}

func podInfoFromPod(pod *apicorev1.Pod) podInfoOrError {
	containerStatuses := append(pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses...)
	containerStates := make([]containerInfo, 0, len(containerStatuses))
	for _, status := range containerStatuses {
		state := containerStateOther
		if status.State.Running != nil {
			state = containerStateRunning
		} else if status.State.Terminated != nil {
			state = containerStateTerminated
		} else if status.State.Waiting != nil {
			state = containerStateWaiting
		}
		containerIDParts := strings.Split(status.ContainerID, "/")
		containerStates = append(containerStates, containerInfo{
			id:    containerIDParts[len(containerIDParts)-1],
			name:  status.Name,
			state: state,
		})
	}
	return podInfoOrError{
		podName:    pod.Name,
		containers: containerStates,
	}
}

/*
 * monitorPods watches for pods in a namespace, and returning a channel of
 * pods that have been modified or some fatal error.
 */
func monitorPods(client *kubernetes.Clientset, namespace string, closer <-chan struct{}) <-chan podInfoOrError {
	result := make(chan podInfoOrError)
	go func() {
		defer close(result)
		pods := client.CoreV1().Pods(namespace)
		var list *apicorev1.PodList
		var err error
		for i := 0; i < 3; i++ {
			list, err = pods.List(metav1.ListOptions{})
			if err == nil {
				break
			}
			statusFunc("Error listing pods", namespace, err)
			time.Sleep(10 * time.Millisecond)
		}
		if err != nil {
			result <- podInfoOrError{err: fmt.Errorf("error listing pods: %w", err)}
			return
		}
		for _, pod := range list.Items {
			result <- podInfoFromPod(&pod)
		}
		watcher, err := pods.Watch(metav1.ListOptions{
			Watch:           true,
			ResourceVersion: list.ResourceVersion,
		})
		if err != nil {
			result <- podInfoOrError{err: fmt.Errorf("error watching pods: %w", err)}
			return
		}
		defer watcher.Stop()
		for {
			select {
			case <-closer:
				return
			case event, ok := <-watcher.ResultChan():
				if !ok {
					// Channel was closed
					return
				}
				switch event.Type {
				case watch.Added, watch.Modified, watch.Deleted:
					pod, ok := event.Object.(*apicorev1.Pod)
					if !ok {
						result <- podInfoOrError{err: fmt.Errorf("could not convert object %v to pod", event.Object.GetObjectKind())}
						return
					}
					result <- podInfoFromPod(pod)
				default:
					result <- podInfoOrError{err: fmt.Errorf("unexpected event type %s", string(event.Type))}
					return
				}
			}
		}
	}()
	return result
}

func watchPod(outputDir, namespace string, pods corev1.PodInterface, podInfo podInfoOrError, containerMatcher *regexp.Regexp) error {
	logRequestLock.Lock()
	if _, ok := logRequests[podInfo.podName]; !ok {
		logRequests[podInfo.podName] = make(map[string]io.ReadCloser)
	}
	logRequestLock.Unlock()

	err := func() error {
		if outputDir != "" {
			outputDir = path.Join(outputDir, namespace, podInfo.podName)
			err := os.MkdirAll(outputDir, os.ModeDir|0755)
			if err != nil {
				return err
			}
		}

		var errorLock sync.Mutex
		var errors []error
		wg := sync.WaitGroup{}

		getOutput := func(container containerInfo) (io.Writer, error) {
			if outputDir == "" {
				// wrap os.Stdout in a MultiWriter to prevent it from being closed
				return io.MultiWriter(os.Stdout), nil
			}

			outPath := path.Join(outputDir, fmt.Sprintf("%s-%s.log", container.name, container.id))
			output, err := os.Create(outPath)
			if err != nil {
				return nil, err
			}
			return output, nil
		}

		copyLogs := func(pods corev1.PodInterface, podName string, container containerInfo, message string) error {
			request := pods.GetLogs(podName, &apicorev1.PodLogOptions{
				Container: container.name,
				Follow:    true,
			})
			reader, err := request.Stream()
			if err != nil {
				return fmt.Errorf("failed to get log stream for %s/%s: %w", podName, container.name, err)
			}
			logRequestLock.Lock()
			logRequests[podName][container.id] = reader
			logRequestLock.Unlock()

			output, err := getOutput(container)
			if err != nil {
				return err
			}
			statusFunc(message, namespace, podName, container.name)
			go func() {
				defer func() {
					if closer, ok := output.(io.Closer); ok {
						closer.Close()
					}
				}()
				_, err := io.Copy(output, reader)
				if err != nil {
					statusFunc("Error copying", namespace, podName, container.name, err)
				}
			}()
			return nil
		}

		for _, container := range podInfo.containers {
			wg.Add(1)
			go func(container containerInfo) {
				defer wg.Done()
				if !containerMatcher.MatchString(container.name) {
					statusFunc("Skipping container", namespace, podInfo.podName, container.name)
					return
				}
				switch container.state {
				case containerStateRunning:
					// Ensure the container is being watched
					logRequestLock.Lock()
					reader := logRequests[podInfo.podName][container.id]
					logRequestLock.Unlock()
					if reader != nil {
						// Container is running, _and_ we have an existing reader.
						// We are already monitoring it, nothing to do here.
						return
					}
					// Container is running, but there's no existing reader.
					// The container just started (or we had logs from a previous run).
					err := copyLogs(pods, podInfo.podName, container, "Starting to watch")
					if err != nil {
						errorLock.Lock()
						errors = append(errors, err)
						errorLock.Unlock()
						return
					}
				case containerStateTerminated:
					// The container is terminated; check if we already have status
					// for it.
					logRequestLock.Lock()
					reader, ok := logRequests[podInfo.podName][container.id]
					logRequestLock.Unlock()
					if reader != nil {
						// We have an active reader; drop it (but leave it running
						// in case we can get more logs).
						logRequestLock.Lock()
						logRequests[podInfo.podName][container.id] = nil
						logRequestLock.Unlock()
						statusFunc("Stopped watching", namespace, podInfo.podName, container.name)
						return
					}
					if ok {
						// Container is terminated; we have already read the logs.
						// Do nothing.
						return
					}
					// We don't have an existing reader; dump the container
					// logs post-mortem.  The container managed to exit before
					// we noticed it.
					err := copyLogs(pods, podInfo.podName, container, "Post-mortem read")
					if err != nil {
						errorLock.Lock()
						errors = append(errors, err)
						errorLock.Unlock()
						return
					}
				default:
					// The container isn't ready to have logs yet.  If we have any
					// existing information, it's about a previous container.
					// Remove the container.
					logRequestLock.Lock()
					reader, ok := logRequests[podInfo.podName][container.id]
					logRequestLock.Unlock()
					if ok {
						logRequestLock.Lock()
						delete(logRequests[podInfo.podName], container.id)
						logRequestLock.Unlock()
						if reader != nil {
							reader.Close()
						}
						statusFunc("Stopped watching", namespace, podInfo.podName, container.name)
					} else {
						statusFunc("Ignoring unknown", namespace, podInfo.podName, container.name)
					}
				}
			}(container)
		}
		wg.Wait()
		if len(errors) > 0 {
			return errors[0]
		}
		return nil
	}()
	if err != nil {
		return fmt.Errorf("failed to watch pod %s: %w", podInfo.podName, err)
	}
	return nil
}
