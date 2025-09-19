package plugin

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/luthermonson/go-proxmox"
	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
	"golang.org/x/sync/errgroup"
)

var (
	_                               provider.InstanceGroup = (*InstanceGroup)(nil)
	ErrTimeoutGettingConnectionInfo                        = errors.New("timed out getting connection info")
	ErrInstanceNotRunning                                  = errors.New("instance is not running")
)

const (
	triggerChannelCapacity = 100
	networkCheckTimeout    = 5 * time.Second
	networkCheckRetries    = 5
)

type InstanceGroup struct {
	Settings         `json:",inline"`
	FleetingSettings provider.Settings `json:"-"`

	log     hclog.Logger    `json:"-"`
	proxmox *proxmox.Client `json:"-"`

	// This mutex is used when cloning template for new instances. It is required for blocking other
	// operations like collection or update, because when new instance is created with recycled ID then for
	// a brief period it will be reported from Proxmox with old name (e.g. InstanceNameRemoving).
	instanceCloningMu sync.Mutex `json:"-"`

	// Trigger for collector to start removed instances collection.
	instanceCollectionTrigger chan struct{} `json:"-"`

	// Trigger to shutdown collector.
	collectorShutdownTrigger chan struct{} `json:"-"`

	// Wait group for the collector.
	collectorWaitGroup sync.WaitGroup `json:"-"`

	// Trigger to shutdown session ticket refresher.
	sessionTicketRefresherShutdownTrigger chan struct{} `json:"-"`

	// Wait group for session ticket refresher.
	sessionTicketRefresherWaitGroup sync.WaitGroup `json:"-"`
}

// Heartbeat implements provider.InstanceGroup.
// It checks if the VM is up and running.
func (ig *InstanceGroup) Heartbeat(ctx context.Context, instance string) error {
	VMID, err := strconv.Atoi(instance)
	if err != nil {
		return fmt.Errorf("failed to parse instance name '%s': %w", instance, err)
	}

	vm, err := ig.getProxmoxVM(ctx, VMID)
	if err != nil {
		return fmt.Errorf("failed to retrieve instance vmid='%d': %w", VMID, err)
	}

	if vm.Uptime == 0 {
		return fmt.Errorf("%w", ErrInstanceNotRunning)
	}

	return nil
}

// Init implements provider.InstanceGroup.
func (ig *InstanceGroup) Init(ctx context.Context, logger hclog.Logger, settings provider.Settings) (provider.ProviderInfo, error) {
	var err error

	ig.log = logger
	ig.FleetingSettings = settings
	ig.instanceCollectionTrigger = make(chan struct{}, triggerChannelCapacity)
	ig.collectorShutdownTrigger = make(chan struct{}, 1)
	ig.sessionTicketRefresherShutdownTrigger = make(chan struct{}, 1)

	err = ig.CheckRequiredFields()
	if err != nil {
		return provider.ProviderInfo{}, err
	}

	ig.FillWithDefaults()

	if ig.InsecureSkipTLSVerify {
		ig.log.Warn("TLS verification for Proxmox client is disabled, connections will be insecure")
	}

	ig.proxmox, err = ig.getProxmoxClient()
	if err != nil {
		return provider.ProviderInfo{}, err
	}

	err = ig.markStaleInstancesForRemoval(ctx)
	if err != nil {
		return provider.ProviderInfo{}, err
	}

	// Sleep for a bit to give Proxmox a chance to propagate renames for stale instances
	// Without this sleep these instances would be reported as creating during first Update
	<-time.After(collectionWaitAfterTrigger)

	//nolint:contextcheck
	ig.startRemovedInstanceCollector()

	//nolint:contextcheck
	ig.startSessionTicketRefresher()

	return provider.ProviderInfo{
		ID:      ig.Pool,
		MaxSize: *ig.MaxInstances,
	}, nil
}

// Shutdown implements provider.InstanceGroup.
func (ig *InstanceGroup) Shutdown(_ context.Context) error {
	ig.collectorShutdownTrigger <- struct{}{}

	ig.sessionTicketRefresherShutdownTrigger <- struct{}{}

	ig.collectorWaitGroup.Wait()
	ig.sessionTicketRefresherWaitGroup.Wait()

	return nil
}

// Increase implements provider.InstanceGroup.
func (ig *InstanceGroup) Increase(ctx context.Context, count int) (int, error) {
	template, err := ig.getProxmoxVM(ctx, *ig.TemplateID)
	if err != nil {
		return 0, fmt.Errorf("failed to find template with id='%d': %w", *ig.TemplateID, err)
	}

	var (
		errorGroup = new(errgroup.Group)

		succeeded   = 0
		succeededMu = new(sync.Mutex)

		// We need to mutex cloning as Proxmox will fail multiple requests in parallel
		cloneMu = new(sync.Mutex)
	)

	ig.instanceCloningMu.Lock()
	defer ig.instanceCloningMu.Unlock()

	for range count {
		errorGroup.Go(func() error {
			vmid, err := ig.deployInstance(ctx, template, cloneMu)
			if err != nil {
				ig.log.Error("failed to deploy an instance", "vmid", vmid, "err", err)
			}

			ig.log.Info("successfully deployed instance", "vmid", vmid)
			succeededMu.Lock()

			succeeded++

			succeededMu.Unlock()

			return err
		})
	}

	err = errorGroup.Wait()
	if err != nil {
		return succeeded, fmt.Errorf("failed to create one or more instances: %w", err)
	}

	return succeeded, nil
}

// Update implements provider.InstanceGroup.
func (ig *InstanceGroup) Update(ctx context.Context, update func(instance string, state provider.State)) error {
	ig.instanceCloningMu.Lock()
	defer ig.instanceCloningMu.Unlock()

	pool, err := ig.getProxmoxPool(ctx)
	if err != nil {
		return err
	}

	for _, member := range pool.Members {
		if !ig.isProxmoxResourceAnInstance(member) {
			continue
		}

		var state provider.State

		switch member.Name {
		case ig.InstanceNameCreating:
			state = provider.StateCreating
		case ig.InstanceNameRunning:
			state = provider.StateRunning
		case ig.InstanceNameRemoving:
			state = provider.StateDeleting
		default:
			continue // Unknown name, skipping...
		}

		update(strconv.FormatUint(member.VMID, 10), state)
	}

	return nil
}

// ConnectInfo implements provider.InstanceGroup.
func (ig *InstanceGroup) ConnectInfo(ctx context.Context, instance string) (provider.ConnectInfo, error) {
	VMID, err := strconv.Atoi(instance)
	if err != nil {
		return provider.ConnectInfo{}, fmt.Errorf("failed to parse instance name '%s': %w", instance, err)
	}

	vm, err := ig.getProxmoxVM(ctx, VMID)
	if err != nil {
		return provider.ConnectInfo{}, fmt.Errorf("failed to retrieve instance vmid='%d': %w", VMID, err)
	}

	return ig.getConnectInfoFromVM(ctx, instance, vm)
}

// Decrease implements provider.InstanceGroup.
func (ig *InstanceGroup) Decrease(ctx context.Context, instancesToRemove []string) ([]string, error) {
	pool, err := ig.getProxmoxPool(ctx)
	if err != nil {
		return []string{}, err
	}

	var (
		errorGroup = new(errgroup.Group)

		succeeded   = []string{}
		succeededMu = new(sync.Mutex)
	)

	for _, member := range pool.Members {
		if !ig.isProxmoxResourceAnInstance(member) {
			continue
		}

		if !slices.Contains(instancesToRemove, strconv.FormatUint(member.VMID, 10)) {
			continue
		}

		if member.Name == ig.InstanceNameCreating {
			// It must be running to start the deletion
			continue
		}

		if member.Name == ig.InstanceNameRemoving {
			// Already deleting...
			succeededMu.Lock()

			succeeded = append(succeeded, strconv.FormatUint(member.VMID, 10))

			succeededMu.Unlock()

			continue
		}

		ig.log.Info("removing instance", "vmid", member.VMID)

		errorGroup.Go(func() error {
			err = ig.markInstancesForRemoval(ctx, &member)
			if err != nil {
				return err
			}

			succeededMu.Lock()
			defer succeededMu.Unlock()

			succeeded = append(succeeded, strconv.FormatUint(member.VMID, 10))

			return nil
		})
	}

	//nolint:wrapcheck
	return succeeded, errorGroup.Wait()
}

func (ig *InstanceGroup) getConnectInfoFromVM(ctx context.Context, instance string, vm *proxmox.VirtualMachine) (provider.ConnectInfo, error) {
	for retry := range networkCheckRetries {
		networkInterfaces, err := vm.AgentGetNetworkIFaces(ctx)
		if err != nil {
			return provider.ConnectInfo{}, fmt.Errorf("failed to retrieve instance vmid='%d' interfaces: %w", vm.VMID, err)
		}

		internalAddress, externalAddress, err := determineAddresses(networkInterfaces, ig.InstanceNetworkInterface, ig.InstanceNetworkProtocol)
		if err != nil {
			ig.log.Error("failed to get network interface", "retry", retry, "err", err)
			time.Sleep(networkCheckTimeout)

			continue
		}

		return provider.ConnectInfo{
			ID:              instance,
			InternalAddr:    internalAddress,
			ExternalAddr:    externalAddress,
			ConnectorConfig: ig.FleetingSettings.ConnectorConfig,
		}, nil
	}

	return provider.ConnectInfo{}, fmt.Errorf("%w", ErrTimeoutGettingConnectionInfo)
}
