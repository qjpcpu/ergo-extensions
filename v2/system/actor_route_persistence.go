package system

import (
	"context"
	"errors"
	"time"

	"ergo.services/ergo/gen"
)

type SessionID string

type RouteOwner struct {
	SessionID SessionID
	PID       gen.PID
}

type ActorRoute struct {
	Key   gen.Atom
	Owner RouteOwner
}

type SessionLease struct {
	SessionID SessionID
	ValidFor  time.Duration
}

type RouteSnapshot struct {
	ActorRoute
	ValidFor     time.Duration
	SessionValid bool
}

type AcquireRouteStatus uint8

const (
	RouteAcquired AcquireRouteStatus = iota + 1
	RouteOccupied
	RouteCompareFailed
)

type AcquireRouteResult struct {
	Status   AcquireRouteStatus
	ValidFor time.Duration
}

var (
	ErrSessionLost     = errors.New("actor route session lost")
	ErrRouteExpired    = errors.New("actor route expired")
	ErrActorRouterBusy = errors.New("actor router busy")
	// ErrRouteNotApplied certifies that an unsuccessful acquisition did not write.
	// Wrap it together with the underlying error using errors.Join.
	ErrRouteNotApplied = errors.New("actor route acquisition not applied")
)

// ActorRoutePersistence owns shared sessions and independently expiring routes.
// Implementations are concurrent-safe and honor contexts. ReadRoute returns one
// consistent route/session snapshot. AcquireRoute atomically checks the requesting
// session and compares the exact expected owner; nil expects an absent route.
// An existing identical owner succeeds and resets RouteTTL. ReleaseRoute compares
// key, session and full PID and succeeds if absent or replaced.
//
// OpenSession always allocates a fresh identity. RenewSession only extends a live
// existing session. CloseSession is idempotent and terminal: concurrent late
// renewals/acquisitions cannot resurrect it. Expired sessions cannot be revived.
// ValidFor is remaining validity at the operation's execution, not a client-side
// timestamp. Backends reclaim expired data even if it is never read again.
//
// Acquisition errors are uncertain unless errors.Is(err, ErrRouteNotApplied).
// Do not implicitly resend an acquisition after an uncertain transport failure.
// Successful operations must not silently roll back within the backend's stated
// failure model. Session renewal must not refresh individual route records.
type ActorRoutePersistence interface {
	OpenSession(ctx context.Context, node gen.Atom, ttl time.Duration) (SessionLease, error)
	RenewSession(ctx context.Context, sessionID SessionID, ttl time.Duration) (SessionLease, error)
	CloseSession(ctx context.Context, sessionID SessionID) error
	ReadRoute(ctx context.Context, key gen.Atom) (RouteSnapshot, bool, error)
	AcquireRoute(ctx context.Context, sessionID SessionID, key gen.Atom, pid gen.PID, expected *RouteOwner, ttl time.Duration) (AcquireRouteResult, error)
	ReleaseRoute(ctx context.Context, sessionID SessionID, key gen.Atom, pid gen.PID) error
}
