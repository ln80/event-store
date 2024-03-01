package internal

import (
	"bytes"
	"fmt"
	"go/format"
	"html/template"
	"os"
	"os/exec"
)

func CheckDir(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}

	return nil
}

func SetupModule(module string, dir string) error {
	cmd := exec.Command("go", "mod", "init", module)
	cmd.Dir = dir
	_, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to init go module: %s", module)
	}
	cmd = exec.Command("go", "mod", "tidy")
	cmd.Dir = dir
	_, err = cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to run 'go mod tidy' in module: %s", module)
	}

	return nil
}

func RenderCode(tmpl *template.Template, data any) ([]byte, error) {
	var buf bytes.Buffer

	if err := tmpl.Execute(&buf, data); err != nil {
		return nil, err
	}
	pretty, err := format.Source(buf.Bytes())
	if err != nil {
		return nil, err
	}

	return pretty, nil
}

func WriteToFile(path string, b []byte) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := file.Write(b); err != nil {
		return err
	}

	return nil
}
