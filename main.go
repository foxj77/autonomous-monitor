package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	metricsv1beta1client "k8s.io/metrics/pkg/client/clientset/versioned"
)

func main() {
	cfg := LoadConfig()
	log.Printf("starting autonomous monitor namespace=%s poll_interval=%s findings_topic=%s", cfg.Namespace, cfg.PollInterval, cfg.FindingsTopic)

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		srv := &http.Server{
			Addr:              ":" + cfg.MetricsPort,
			Handler:           mux,
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       30 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       2 * time.Minute,
		}
		log.Printf("metrics server listening on :%s", cfg.MetricsPort)
		log.Fatal(srv.ListenAndServe())
	}()

	kube, restCfg, err := buildKubeClient()
	if err != nil {
		log.Fatalf("failed to build Kubernetes client: %v", err)
	}

	var metricsClient metricsv1beta1client.Interface
	if cfg.Checks.ResourceUsage && cfg.ResourceUsageBackend == "metrics-server" {
		mc, err := metricsv1beta1client.NewForConfig(restCfg)
		if err != nil {
			log.Fatalf("failed to build metrics client: %v", err)
		}
		metricsClient = mc
		log.Printf("resource usage checks enabled backend=metrics-server")
	}

	var dynClient dynamic.Interface
	if cfg.Checks.CustomResource {
		dc, err := dynamic.NewForConfig(restCfg)
		if err != nil {
			log.Fatalf("failed to build dynamic client: %v", err)
		}
		dynClient = dc
	}

	publisher, err := NewKafkaPublisher(cfg.RedpandaBroker, cfg.FindingsTopic, cfg.PublishTimeout)
	if err != nil {
		log.Fatalf("failed to create Redpanda publisher: %v", err)
	}
	defer publisher.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	store := NewStateStore(kube, cfg.Namespace, cfg.StateConfigMapName)
	state, err := store.Load(ctx)
	if err != nil {
		log.Printf("WARN: state load issue: %v", err)
	}
	if state == nil {
		state = newMonitorState(cfg.Namespace)
	}

	monitor := NewMonitor(cfg, kube, metricsClient, dynClient, publisher, store, state)
	monitor.Poll(ctx)

	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			monitor.maybeSave(context.Background(), time.Now().UTC().Add(cfg.StateWriteInterval))
			log.Println("shutting down autonomous monitor")
			return
		case <-ticker.C:
			monitor.Poll(ctx)
		}
	}
}

func buildKubeClient() (kubernetes.Interface, *rest.Config, error) {
	cfg, err := rest.InClusterConfig()
	if err == nil {
		client, err := kubernetes.NewForConfig(cfg)
		return client, cfg, err
	}

	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		home, homeErr := os.UserHomeDir()
		if homeErr == nil {
			kubeconfig = filepath.Join(home, ".kube", "config")
		}
	}

	cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, nil, err
	}
	client, err := kubernetes.NewForConfig(cfg)
	return client, cfg, err
}
