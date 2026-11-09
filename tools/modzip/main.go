// Copyright 2024-2026 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Command modzip writes the Go module zip of a tagged revision of the
// repository, in the layout the GOPROXY protocol serves
// (<module>@<version>/...). The release workflow uploads it to JFrog.
//
// The zip is built from git, not from the working tree, by the same
// golang.org/x/mod/zip package the Go module proxy uses. Files that are
// untracked or generated in the working tree therefore never reach the zip,
// and the module hash matches the h1: sum consumers get from proxy.golang.org.
//
// This tool is a separate module on purpose: it keeps golang.org/x/mod out of
// the library's direct requirements, and the Go module zip format excludes
// nested modules, so the tool never ships inside the library zip.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
	"golang.org/x/mod/zip"
)

// errVersionRequired is returned when the -version flag is empty.
var errVersionRequired = errors.New("version is required")

// config holds the command line options.
type config struct {
	// version is the module version to pack, with the "v" prefix.
	version string
	// revision is the git revision to pack. Defaults to version.
	revision string
	// repo is the path to the repository worktree root.
	repo string
	// out is the directory that receives <version>.zip.
	out string
}

func main() {
	var cfg config

	flag.StringVar(&cfg.version, "version", "",
		"module version to pack, with the v prefix (e.g. v0.13.0)")
	flag.StringVar(&cfg.revision, "revision", "",
		"git revision to pack (default: the version tag)")
	flag.StringVar(&cfg.repo, "repo", ".", "path to the repository worktree root")
	flag.StringVar(&cfg.out, "out", "dist", "directory to write <version>.zip into")
	flag.Parse()

	dest, modVersion, err := run(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "modzip: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("%s -> %s@%s\n", dest, modVersion.Path, modVersion.Version)
}

// run packs the module zip described by cfg and returns the path of the
// written file and the packed module version.
func run(cfg config) (string, module.Version, error) {
	if cfg.version == "" {
		return "", module.Version{}, errVersionRequired
	}

	if cfg.revision == "" {
		cfg.revision = cfg.version
	}

	repoRoot, err := filepath.Abs(cfg.repo)
	if err != nil {
		return "", module.Version{}, fmt.Errorf("failed to resolve repo path %q: %w", cfg.repo, err)
	}

	modPath, err := modulePath(filepath.Join(repoRoot, "go.mod"))
	if err != nil {
		return "", module.Version{}, err
	}

	modVersion := module.Version{Path: modPath, Version: cfg.version}
	if err = module.Check(modVersion.Path, modVersion.Version); err != nil {
		return "", module.Version{}, fmt.Errorf("failed to validate module version: %w", err)
	}

	if err = os.MkdirAll(cfg.out, 0o750); err != nil {
		return "", module.Version{}, fmt.Errorf("failed to create output dir %q: %w", cfg.out, err)
	}

	dest := filepath.Join(cfg.out, cfg.version+".zip")
	if err = writeZip(dest, modVersion, repoRoot, cfg.revision); err != nil {
		return "", module.Version{}, err
	}

	return dest, modVersion, nil
}

// writeZip creates the module zip at dest from the given git revision.
// A partially written file is removed on failure.
func writeZip(dest string, modVersion module.Version, repoRoot, revision string) (err error) {
	f, err := os.Create(dest)
	if err != nil {
		return fmt.Errorf("failed to create %q: %w", dest, err)
	}

	defer func() {
		if cerr := f.Close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("failed to close %q: %w", dest, cerr))
		}

		if err != nil {
			if rerr := os.Remove(dest); rerr != nil {
				err = errors.Join(err, fmt.Errorf("failed to remove %q: %w", dest, rerr))
			}
		}
	}()

	if err = zip.CreateFromVCS(f, modVersion, repoRoot, revision, ""); err != nil {
		return fmt.Errorf("failed to create module zip for %s: %w", modVersion, err)
	}

	return nil
}

// modulePath reads the module path from the go.mod file at goMod.
func modulePath(goMod string) (string, error) {
	data, err := os.ReadFile(goMod)
	if err != nil {
		return "", fmt.Errorf("failed to read %q: %w", goMod, err)
	}

	path := modfile.ModulePath(data)
	if path == "" {
		return "", fmt.Errorf("no module path in %q", goMod)
	}

	return path, nil
}
