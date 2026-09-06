package membership

import (
	"math/rand"
	"strconv"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	core "github.com/qjpcpu/ergo-extensions/v2/system/internal/core"
	"github.com/qjpcpu/registrar/events"
)

// ProcessName is the registered name of the membership process.
const ProcessName = gen.Atom("extensions_membership")

// Options controls membership refresh, debounce, and retry timing.
type Options struct {
	RefreshInterval time.Duration
	DebounceMin     time.Duration
	DebounceMax     time.Duration
	RetryMin        time.Duration
	RetryMax        time.Duration
}

// DefaultOptions returns balanced membership timing defaults.
func DefaultOptions() Options {
	return Options{
		RefreshInterval: 30 * time.Second,
		DebounceMin:     100 * time.Millisecond,
		DebounceMax:     2 * time.Second,
		RetryMin:        time.Second,
		RetryMax:        30 * time.Second,
	}
}

func normalizeOptions(options Options) Options {
	defaults := DefaultOptions()
	if options.RefreshInterval <= 0 {
		options.RefreshInterval = defaults.RefreshInterval
	}
	if options.DebounceMin <= 0 {
		options.DebounceMin = defaults.DebounceMin
	}
	if options.DebounceMax <= 0 {
		options.DebounceMax = defaults.DebounceMax
	}
	if options.DebounceMax < options.DebounceMin {
		options.DebounceMax = options.DebounceMin
	}
	if options.RetryMin <= 0 {
		options.RetryMin = defaults.RetryMin
	}
	if options.RetryMax <= 0 {
		options.RetryMax = defaults.RetryMax
	}
	if options.RetryMax < options.RetryMin {
		options.RetryMax = options.RetryMin
	}
	return options
}

type messageInit struct{}

type messageRefresh struct {
	ID int64
}

type messageTopologyChanged struct {
	ID int64
}

type membership struct {
	act.Actor

	book      *core.AddressBook
	registrar gen.Registrar
	event     gen.Event
	options   Options

	notifiedVersion int64
	topologyDirty   bool
	refreshID       int64
	topologyID      int64
	retry           int
	lastError       error
	lastUpdate      time.Time

	cancelRefresh  gen.CancelFunc
	cancelTopology gen.CancelFunc
}

// Factory creates a membership process factory.
func Factory(book *core.AddressBook, options Options) gen.ProcessFactory {
	options = normalizeOptions(options)
	return func() gen.ProcessBehavior {
		return &membership{book: book, options: options}
	}
}

func (m *membership) Init(args ...any) error {
	return m.Send(m.PID(), messageInit{})
}

func (m *membership) HandleMessage(from gen.PID, message any) error {
	switch msg := message.(type) {
	case messageInit:
		if err := m.setup(); err != nil {
			m.recordFailure(err)
			m.scheduleRefresh(m.retryDelay())
			return nil
		}
		m.refreshAndSchedule()
	case messageRefresh:
		if msg.ID == m.refreshID {
			m.cancelRefresh = nil
			m.refreshAndSchedule()
		}
	case messageTopologyChanged:
		if msg.ID == m.topologyID {
			m.cancelTopology = nil
			m.refreshAndSchedule()
		}
	}
	return nil
}

func (m *membership) HandleEvent(message gen.MessageEvent) error {
	switch message.Message.(type) {
	case events.EventNodeJoined, events.EventNodeLeft:
		m.topologyDirty = true
		m.scheduleTopologyRefresh()
	}
	return nil
}

func (m *membership) HandleInspect(from gen.PID, item ...string) map[string]string {
	result := map[string]string{
		"nodes":        strconv.Itoa(m.book.GetAvailableNodes().Len()),
		"last_refresh": m.lastUpdate.Format(time.RFC3339Nano),
	}
	if m.lastError != nil {
		result["last_error"] = m.lastError.Error()
	}
	return result
}

func (m *membership) Terminate(reason error) {
	if m.cancelRefresh != nil {
		m.cancelRefresh()
		m.cancelRefresh = nil
	}
	if m.cancelTopology != nil {
		m.cancelTopology()
		m.cancelTopology = nil
	}
	if m.event.Name != "" {
		_ = m.DemonitorEvent(m.event)
	}
}

func (m *membership) setup() error {
	if m.registrar != nil {
		return nil
	}
	registrar, err := m.Node().Network().Registrar()
	if err != nil {
		return err
	}
	event, err := registrar.Event()
	if err != nil {
		return err
	}
	if _, err := m.MonitorEvent(event); err != nil {
		return err
	}
	m.registrar = registrar
	m.event = event
	return nil
}

func (m *membership) refreshAndSchedule() {
	if err := m.refresh(); err != nil {
		m.recordFailure(err)
		m.scheduleRefresh(m.retryDelay())
		return
	}
	m.retry = 0
	m.lastError = nil
	m.lastUpdate = time.Now().UTC()
	m.scheduleRefresh(m.options.RefreshInterval)
}

func (m *membership) refresh() error {
	if m.registrar == nil {
		if err := m.setup(); err != nil {
			return err
		}
	}
	nodes, err := m.registrar.Nodes()
	if err != nil {
		return err
	}
	nodes = append(nodes, m.Node().Name())
	nodeList := core.NewNodeList(nodes...)
	if err := m.book.SetAvailableNodes(nodeList); err != nil {
		return err
	}
	if m.topologyDirty || m.notifiedVersion != m.book.NodesVersion() {
		if err := m.Send(gen.Atom("extensions_daemon"), core.MessageTopologyUpdated{}); err != nil {
			return err
		}
		m.notifiedVersion = m.book.NodesVersion()
		m.topologyDirty = false
	}
	return nil
}

func (m *membership) recordFailure(err error) {
	m.lastError = err
	m.retry++
	if m.retry == 1 || m.retry%10 == 0 {
		m.Log().Warning("membership refresh failed; repeated failures are rate limited: %v", err)
	}
}

func (m *membership) retryDelay() time.Duration {
	delay := m.options.RetryMin
	for attempt := 1; attempt < m.retry && delay < m.options.RetryMax; attempt++ {
		delay *= 2
	}
	if delay > m.options.RetryMax {
		return m.options.RetryMax
	}
	return delay
}

func (m *membership) scheduleRefresh(delay time.Duration) {
	if m.cancelRefresh != nil {
		m.cancelRefresh()
	}
	m.refreshID++
	cancel, err := m.SendAfter(m.PID(), messageRefresh{ID: m.refreshID}, delay)
	if err == nil {
		m.cancelRefresh = cancel
	}
}

func (m *membership) scheduleTopologyRefresh() {
	if m.cancelTopology != nil {
		m.cancelTopology()
	}
	m.topologyID++
	delay := m.options.DebounceMin
	window := m.options.DebounceMax - m.options.DebounceMin
	if window > 0 {
		delay += time.Duration(rand.Int63n(int64(window) + 1))
	}
	cancel, err := m.SendAfter(m.PID(), messageTopologyChanged{ID: m.topologyID}, delay)
	if err == nil {
		m.cancelTopology = cancel
	}
}
