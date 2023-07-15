package master

import (
	"fmt"
	"gfs"
	"sync"

	log "github.com/sirupsen/logrus"
)

type namespaceManager struct {
	root *nsTree
}

type nsTree struct {
	sync.RWMutex

	// if it is a directory
	isDir    bool
	children map[string]*nsTree

	// if it is a file
	length int64
	chunks int64
}

func newNamespaceManager() *namespaceManager {
	nm := &namespaceManager{
		root: &nsTree{
			isDir:    true,
			children: make(map[string]*nsTree),
		},
	}
	return nm
}

// acquire read lock along the parents (e.g. /d1/d2/.../dn/leaf):
// 
// acquire read-locks on the directory names /d1, /d1/d2, ..., /d1/d2/.../dn
// 
// If RLockLeaf = True, then acquire read-locks on /d1/d2/.../dn/leaf
func (nm *namespaceManager) lockParents(paths []string, RLockLeaf bool) (*nsTree, error) {
	// fmt.Println(paths)
	// fmt.Println(len(paths))
	root := nm.root
	if len(paths) > 0 {
		for _, p := range paths {
			ch, ok := root.children[p]
			if !ok {
				return nil, fmt.Errorf("path %v doesn't exist", paths)
			}
			root.RLock()
			root = ch
		}
	}
	if !root.isDir {
		return nil, fmt.Errorf("path %v is not a directory", paths[len(paths) - 1])
	}
	if RLockLeaf {
		root.RLock()
	}
	return root, nil
}

// unlock read lock along the parents
func (nm *namespaceManager) unlockParents(paths []string, RUnlockLeaf bool) {
	root := nm.root
	if len(paths) > 0 {
		for _, p := range paths {
			ch := root.children[p]
			root.RUnlock()
			root = ch
		}
	}
	if RUnlockLeaf {
		root.RUnlock()
	}
}

// Create creates an empty file on path p. All parents should exist.
func (nm *namespaceManager) Create(p gfs.Path) error {
	path, filename := p.ParseLeafname()
	log.Info("Create: ", path, "/", filename)
	paths := path.GetPaths()
	node, err := nm.lockParents(paths, false)
	defer nm.unlockParents(paths, false)
	if err != nil {
		return err
	}
	
	node.Lock()
	defer node.Unlock()
	
	// log.Printf("%v has content: %v %v", path, node, filename)
	if _, ok := node.children[filename]; ok {
		return fmt.Errorf("file %v already exists", filename)
	}
	node.children[filename] = &nsTree{
		isDir: false,
	}
	
	return nil
}

// Mkdir creates a directory on path p. All parents should exist.
func (nm *namespaceManager) Mkdir(p gfs.Path) error {
	path, dirname := p.ParseLeafname()
	log.Info("Mkdir: ", path, "/", dirname)
	paths := path.GetPaths()
	node, err := nm.lockParents(paths, false)
	defer nm.unlockParents(paths, false)
	if err != nil {
		return err
	}

	node.Lock()
	defer node.Unlock()

	// log.Printf("%v has content: %v %v", path, node, dirname)
	if _, ok := node.children[dirname]; ok {
		return fmt.Errorf("directory %v already exists", dirname)
	}
	node.children[dirname] = &nsTree{
		isDir: true,
		children: make(map[string]*nsTree),
	}
	return nil
}

// GetFileInfo returns the info of file, including length, chunks, isDir
func (nm *namespaceManager) GetFileInfo(p gfs.Path, reply *gfs.GetFileInfoReply) error {
	path, filename := p.ParseLeafname()
	log.Info("GetFileInfo: ", path, "/", filename)
	paths := path.GetPaths()
	node, err := nm.lockParents(paths, true)
	defer nm.unlockParents(paths, true)
	if err != nil {
		return err
	}
	
	file, ok := node.children[filename]
	if !ok {
		return fmt.Errorf("file %v doesn't exist", filename)
	}

	reply.IsDir = file.isDir
	reply.Chunks = file.chunks
	reply.Length = file.length

	return nil
}

func (nm *namespaceManager) List(p gfs.Path) ([]gfs.PathInfo, error) {
	log.Info("List: ", p)
	paths := p.GetPaths()
	node, err := nm.lockParents(paths, true)
	defer nm.unlockParents(paths, true)
	if err != nil {
		return nil, err
	}

	leaves := make([]gfs.PathInfo, 0, len(node.children))
	for name, leaf := range node.children {
		log.Printf("%v %v", &node, name)
		leaves = append(leaves, gfs.PathInfo{
			Name: name,
			IsDir: leaf.isDir,
			Length: leaf.length,
			Chunks: leaf.chunks,
		})
	}

	return leaves, nil
}