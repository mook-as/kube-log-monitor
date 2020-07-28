package cmd

import (
	"fmt"
	"os"
	"regexp"

	"github.com/mook-as/kube-log-monitor/pkg/kubelogmonitor"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "kube-log-monitor",
	Short: "Monitor kubernetes logs",
	Long:  `Monitors kubernetes logs, for use with init containers.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := kubelogmonitor.GetClient(viper.GetString("kubeconfig"), viper.GetString("context"))
		if err != nil {
			return err
		}
		if client == nil {
			return fmt.Errorf("could not get client")
		}
		pod, err := regexp.Compile(viper.GetString("pod"))
		if err != nil {
			return fmt.Errorf("could not compile pod matcher: %w", err)
		}
		container, err := regexp.Compile(viper.GetString("container"))
		if err != nil {
			return fmt.Errorf("could not compile container matcher: %w", err)
		}

		err = kubelogmonitor.Monitor(client, viper.GetString("output"), viper.GetStringSlice("namespaces"), pod, container)

		if err != nil {
			return err
		}
		return nil
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(func() {
		viper.SetEnvPrefix("")
		viper.AutomaticEnv()
	})

	var kubeconfig, context string
	persistentFlags := rootCmd.PersistentFlags()
	persistentFlags.StringVar(&kubeconfig, "kubeconfig", "", "Kubernetes config file")
	persistentFlags.StringVar(&context, "context", "", "Kubernetes context")
	viper.BindPFlag("kubeconfig", persistentFlags.Lookup("kubeconfig"))
	viper.BindPFlag("context", persistentFlags.Lookup("context"))

	wd, err := os.Getwd()
	if err != nil {
		wd = ""
	}

	var namespace []string
	var pod, container, output string
	flags := rootCmd.Flags()
	flags.StringSliceVar(&namespace, "namespaces", []string{"scf"}, "Namespaces to monitor")
	flags.StringVar(&pod, "pod", ".*", "Pods to match")
	flags.StringVar(&container, "container", ".*", "Containers to match")
	flags.StringVar(&output, "output", wd, "directory to output logs")
	viper.BindPFlag("namespaces", flags.Lookup("namespaces"))
	viper.BindPFlag("pod", flags.Lookup("pod"))
	viper.BindPFlag("container", flags.Lookup("container"))
	viper.BindPFlag("output", flags.Lookup("output"))
}
