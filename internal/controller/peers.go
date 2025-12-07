package controller

// PeerInfo describes a Raft peer.
type PeerInfo struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}
