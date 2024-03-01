package fs

import (
	"io/fs"
	"os"
)

type Adapter struct {
	fs  fs.FS
	dir string
}

func NewAdapter(f fs.FS, dir string) *Adapter {
	a := &Adapter{
		fs:  f,
		dir: dir,
	}

	return a
}

func NewDirAdapter(dir string) *Adapter {
	return NewAdapter(os.DirFS(dir), dir)
}
