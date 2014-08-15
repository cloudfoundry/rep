package prune

import db "github.com/cloudfoundry/storeadapter"

var token = struct{}{}

func Prune(store db.StoreAdapter, rootKey string, predicate func(db.StoreNode) bool) error {
	rootNode, err := store.ListRecursively(rootKey)
	if err != nil && err != db.ErrorKeyNotFound {
		return err
	}

	p := NewPruner(rootNode, predicate)
	keySetsToDelete := p.Prune()

	// note: we don't want to delete the root node, so do not delete the 0 index key set
	for i := len(keySetsToDelete) - 1; i > 0; i-- {
		if len(keySetsToDelete[i]) > 0 {
			store.DeleteLeaves(keySetsToDelete[i]...)
		}
	}

	return nil
}

type Pruner struct {
	root            db.StoreNode
	keySetsToDelete [][]string
	predicate       func(db.StoreNode) bool
}

func NewPruner(root db.StoreNode, predicate func(db.StoreNode) bool) *Pruner {
	return &Pruner{
		root:      root,
		predicate: predicate,
	}
}

func (p *Pruner) Prune() (keySetsToDelete [][]string) {
	p.walk(0, p.root)
	return p.keySetsToDelete
}

func (p *Pruner) walk(depth int, node db.StoreNode) bool {
	if len(p.keySetsToDelete) < depth+1 {
		p.keySetsToDelete = append(p.keySetsToDelete, []string{})
	}

	if len(node.ChildNodes) == 0 {
		if node.Dir || !p.predicate(node) {
			p.markForDelete(depth, node.Key)
			return false
		} else {
			return true
		}
	}

	empty := true
	childDepth := depth + 1
	for _, childNode := range node.ChildNodes {
		if p.walk(childDepth, childNode) {
			empty = false
		}
	}

	if empty {
		p.markForDelete(depth, node.Key)
		return false
	}

	return true
}

func (p *Pruner) markForDelete(depth int, key string) {
	p.keySetsToDelete[depth] = append(p.keySetsToDelete[depth], key)
}
