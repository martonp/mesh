package tatanka

import (
	"net"
	"testing"
	"time"

	igd "github.com/emersion/go-upnp-igd"
)

func TestNATMapperRefreshExternalIPUpdatesAddressWithoutRenewingMapping(t *testing.T) {
	device := &testNATDevice{
		externalIP: net.ParseIP("5.6.7.8"),
	}
	mapper := &natMapper{
		log:        newTestLogger(),
		listenPort: 1234,
		device:     device,
		externalIP: net.ParseIP("1.2.3.4"),
		mappedPort: 1234,
	}

	mapper.refreshExternalIP()

	if device.getExternalIPCalls != 1 {
		t.Fatalf("expected 1 external IP poll, got %d", device.getExternalIPCalls)
	}
	if device.addPortMappingCalls != 0 {
		t.Fatalf("expected 0 port mapping renewals, got %d", device.addPortMappingCalls)
	}

	pubAddr := mapper.publicAddr()
	if pubAddr == nil || pubAddr.String() != "/ip4/5.6.7.8/tcp/1234" {
		t.Fatalf("unexpected public addr after IP refresh: %v", pubAddr)
	}
}

func TestNATMapperRenewMappingUpdatesPortWithoutPollingExternalIP(t *testing.T) {
	device := &testNATDevice{
		mappedPort: 4321,
	}
	mapper := &natMapper{
		log:        newTestLogger(),
		listenPort: 1234,
		device:     device,
		externalIP: net.ParseIP("1.2.3.4"),
		mappedPort: 1234,
	}

	mapper.renewMapping()

	if device.addPortMappingCalls != 1 {
		t.Fatalf("expected 1 port mapping renewal, got %d", device.addPortMappingCalls)
	}
	if device.getExternalIPCalls != 0 {
		t.Fatalf("expected 0 external IP polls, got %d", device.getExternalIPCalls)
	}
	if device.lastInternalPort != 1234 || device.lastExternalPort != 1234 {
		t.Fatalf("unexpected mapping request ports: internal=%d external=%d", device.lastInternalPort, device.lastExternalPort)
	}

	pubAddr := mapper.publicAddr()
	if pubAddr == nil || pubAddr.String() != "/ip4/1.2.3.4/tcp/4321" {
		t.Fatalf("unexpected public addr after mapping renewal: %v", pubAddr)
	}
}

type testNATDevice struct {
	externalIP net.IP
	mappedPort int

	getExternalIPCalls     int
	addPortMappingCalls    int
	deletePortMappingCalls int
	lastInternalPort       int
	lastExternalPort       int
}

func (d *testNATDevice) GetExternalIPAddress() (net.IP, error) {
	d.getExternalIPCalls++
	return append(net.IP(nil), d.externalIP...), nil
}

func (d *testNATDevice) AddPortMapping(_ igd.Protocol, internalPort, externalPort int, _ string, _ time.Duration) (int, error) {
	d.addPortMappingCalls++
	d.lastInternalPort = internalPort
	d.lastExternalPort = externalPort
	if d.mappedPort != 0 {
		return d.mappedPort, nil
	}
	return externalPort, nil
}

func (d *testNATDevice) DeletePortMapping(_ igd.Protocol, _ int) error {
	d.deletePortMappingCalls++
	return nil
}
