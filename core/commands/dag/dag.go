package dagcmd

import (
	"errors"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/ipfs/go-ipfs/core/commands/cmdenv"
	"github.com/ipfs/go-ipfs/core/coredag"

	gocar "github.com/ipfs/go-car"
	cid "github.com/ipfs/go-cid"
	cidenc "github.com/ipfs/go-cidutil/cidenc"
	cmds "github.com/ipfs/go-ipfs-cmds"
	files "github.com/ipfs/go-ipfs-files"
	ipld "github.com/ipfs/go-ipld-format"
	ipfspath "github.com/ipfs/go-path"
	path "github.com/ipfs/interface-go-ipfs-core/path"
	mh "github.com/multiformats/go-multihash"
)

const (
	//progressOptionName = "progress"
	//quietOptionName    = "quiet"
	silentOptionName   = "silent"
	pinRootsOptionName = "pin-roots"
)

var DagCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Interact with ipld dag objects.",
		ShortDescription: `
'ipfs dag' is used for creating and manipulating dag objects.

This subcommand is currently an experimental feature, but it is intended
to deprecate and replace the existing 'ipfs object' command moving forward.
		`,
	},
	Subcommands: map[string]*cmds.Command{
		"put":     DagPutCmd,
		"get":     DagGetCmd,
		"resolve": DagResolveCmd,
		"import":  DagImportCmd,
	},
}

// OutputObject is the output type of 'dag put'/'dag import'command
type OutputObject struct {
	Cid cid.Cid
}

// ResolveOutput is the output type of 'dag resolve' command
type ResolveOutput struct {
	Cid     cid.Cid
	RemPath string
}

var DagPutCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Add a dag node to ipfs.",
		ShortDescription: `
'ipfs dag put' accepts input from a file or stdin and parses it
into an object of the specified format.
`,
	},
	Arguments: []cmds.Argument{
		cmds.FileArg("object data", true, true, "The object to put").EnableStdin(),
	},
	Options: []cmds.Option{
		cmds.StringOption("format", "f", "Format that the object will be added as.").WithDefault("cbor"),
		cmds.StringOption("input-enc", "Format that the input object will be.").WithDefault("json"),
		cmds.BoolOption("pin", "Pin this object when adding."),
		cmds.StringOption("hash", "Hash function to use").WithDefault(""),
	},
	Run: func(req *cmds.Request, res cmds.ResponseEmitter, env cmds.Environment) error {
		api, err := cmdenv.GetApi(env, req)
		if err != nil {
			return err
		}

		ienc, _ := req.Options["input-enc"].(string)
		format, _ := req.Options["format"].(string)
		hash, _ := req.Options["hash"].(string)
		dopin, _ := req.Options["pin"].(bool)

		// mhType tells inputParser which hash should be used. MaxUint64 means 'use
		// default hash' (sha256 for cbor, sha1 for git..)
		mhType := uint64(math.MaxUint64)

		if hash != "" {
			var ok bool
			mhType, ok = mh.Names[hash]
			if !ok {
				return fmt.Errorf("%s in not a valid multihash name", hash)
			}
		}

		var adder ipld.NodeAdder = api.Dag()
		if dopin {
			adder = api.Dag().Pinning()
		}
		b := ipld.NewBatch(req.Context, adder)

		it := req.Files.Entries()
		for it.Next() {
			file := files.FileFromEntry(it)
			if file == nil {
				return fmt.Errorf("expected a regular file")
			}
			nds, err := coredag.ParseInputs(ienc, format, file, mhType, -1)
			if err != nil {
				return err
			}
			if len(nds) == 0 {
				return fmt.Errorf("no node returned from ParseInputs")
			}

			for _, nd := range nds {
				err := b.Add(req.Context, nd)
				if err != nil {
					return err
				}
			}

			cid := nds[0].Cid()
			if err := res.Emit(&OutputObject{Cid: cid}); err != nil {
				return err
			}
		}
		if it.Err() != nil {
			return it.Err()
		}

		if err := b.Commit(); err != nil {
			return err
		}

		return nil
	},
	Type: OutputObject{},
	Encoders: cmds.EncoderMap{
		cmds.Text: cmds.MakeTypedEncoder(func(req *cmds.Request, w io.Writer, out *OutputObject) error {
			enc, err := cmdenv.GetLowLevelCidEncoder(req)
			if err != nil {
				return err
			}
			fmt.Fprintln(w, enc.Encode(out.Cid))
			return nil
		}),
	},
}

var DagGetCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Get a dag node from ipfs.",
		ShortDescription: `
'ipfs dag get' fetches a dag node from ipfs and prints it out in the specified
format.
`,
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("ref", true, false, "The object to get").EnableStdin(),
	},
	Run: func(req *cmds.Request, res cmds.ResponseEmitter, env cmds.Environment) error {
		api, err := cmdenv.GetApi(env, req)
		if err != nil {
			return err
		}

		rp, err := api.ResolvePath(req.Context, path.New(req.Arguments[0]))
		if err != nil {
			return err
		}

		obj, err := api.Dag().Get(req.Context, rp.Cid())
		if err != nil {
			return err
		}

		var out interface{} = obj
		if len(rp.Remainder()) > 0 {
			rem := strings.Split(rp.Remainder(), "/")
			final, _, err := obj.Resolve(rem)
			if err != nil {
				return err
			}
			out = final
		}
		return cmds.EmitOnce(res, &out)
	},
}

// DagResolveCmd returns address of highest block within a path and a path remainder
var DagResolveCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Resolve ipld block",
		ShortDescription: `
'ipfs dag resolve' fetches a dag node from ipfs, prints it's address and remaining path.
`,
	},
	Arguments: []cmds.Argument{
		cmds.StringArg("ref", true, false, "The path to resolve").EnableStdin(),
	},
	Run: func(req *cmds.Request, res cmds.ResponseEmitter, env cmds.Environment) error {
		api, err := cmdenv.GetApi(env, req)
		if err != nil {
			return err
		}

		rp, err := api.ResolvePath(req.Context, path.New(req.Arguments[0]))
		if err != nil {
			return err
		}

		return cmds.EmitOnce(res, &ResolveOutput{
			Cid:     rp.Cid(),
			RemPath: rp.Remainder(),
		})
	},
	Encoders: cmds.EncoderMap{
		cmds.Text: cmds.MakeTypedEncoder(func(req *cmds.Request, w io.Writer, out *ResolveOutput) error {
			var (
				enc cidenc.Encoder
				err error
			)
			switch {
			case !cmdenv.CidBaseDefined(req):
				// Not specified, check the path.
				enc, err = cmdenv.CidEncoderFromPath(req.Arguments[0])
				if err == nil {
					break
				}
				// Nope, fallback on the default.
				fallthrough
			default:
				enc, err = cmdenv.GetLowLevelCidEncoder(req)
				if err != nil {
					return err
				}
			}
			p := enc.Encode(out.Cid)
			if out.RemPath != "" {
				p = ipfspath.Join([]string{p, out.RemPath})
			}

			fmt.Fprint(w, p)
			return nil
		}),
	},
	Type: ResolveOutput{},
}

var DagImportCmd = &cmds.Command{
	Helptext: cmds.HelpText{
		Tagline: "Import the contents of .car files",
		ShortDescription: `
'ipfs dag import' parses .car files and adds all contained objects to the blockstore.
`,
	},

	Arguments: []cmds.Argument{
		// Q: what is the EnableRecursive for in the context of paths ?
		cmds.FileArg("path", true, true, "The path of a .car file.").EnableStdin(),
	},
	Options: []cmds.Option{
		// FIXME - need to copy the progress bar from core/commands/add.go
		// Q: Is the progress bar implementation "legit"? Also is "bar per car" ok?
		//cmds.BoolOption(progressOptionName, "p", "Stream progress data.").WithDefault(true),
		//cmds.BoolOption(quietOptionName, "q", "Write minimal output."),
		cmds.BoolOption(silentOptionName, "Write no output."),
		cmds.BoolOption(pinRootsOptionName, "Pin roots optionally listed in the .car header when adding.").WithDefault(true),
	},
	// FIXME - progress bar option disabled above
	// PreRun: func(req *cmds.Request, env cmds.Environment) error {
	// 	quiet, _ := req.Options[quietOptionName].(bool)
	// 	silent, _ := req.Options[silentOptionName].(bool)

	// 	if quiet || silent {
	// 		return nil
	// 	}

	// 	// ipfs cli progress bar defaults to true unless quiet or silent is used
	// 	_, found := req.Options[progressOptionName].(bool)
	// 	if !found {
	// 		req.Options[progressOptionName] = true
	// 	}

	// 	return nil
	// },
	Type: OutputObject{},
	Run: func(req *cmds.Request, res cmds.ResponseEmitter, env cmds.Environment) error {
		api, err := cmdenv.GetApi(env, req)
		if err != nil {
			return err
		}

		doPinRoots, _ := req.Options[pinRootsOptionName].(bool)
		silent, _ := req.Options[silentOptionName].(bool)

		keepTrackOfRoots := doPinRoots || !silent

		// It is not guaranteed that a root in a header is actually present in the same ( or any .car )
		// Accumulate any root CID seen in a header, and supplement its actual node if/when encountered
		// We will pin *only* at the end in case the entire operation is successful
		// Q: - way to do above better?
		var expectRoots map[cid.Cid]*ipld.Node

		if keepTrackOfRoots {
			expectRoots = make(map[cid.Cid]*ipld.Node, 32)
		}

		// If more than one file - parse all headers first
		// ( due to misdesign we do "reparse" each car's header twice, but that's cheap )
		// Q: there seems to be no management of file closures...
		argCount, _ := req.Files.Size()
		if keepTrackOfRoots && argCount > 1 {

			it := req.Files.Entries()
			for it.Next() {

				file := files.FileFromEntry(it)
				if file == nil {
					return errors.New("expected a file")
				}

				car, err := gocar.NewCarReader(file)
				if err != nil {
					return err
				}

				for _, cid := range car.Header.Roots {
					if _, exists := expectRoots[cid]; !exists {
						expectRoots[cid] = nil
					}
				}

				// rewind - we will come back
				file.Seek(0, 0)
			}
		}

		adder := api.Dag()
		b := ipld.NewBatch(req.Context, adder)

		it := req.Files.Entries()
		for it.Next() {

			file := files.FileFromEntry(it)
			if file == nil {
				return errors.New("expected a file")
			}
			defer func() { file.Close() }()

			car, err := gocar.NewCarReader(file)
			if err != nil {
				return err
			}

			if car.Header.Version != 1 {
				return errors.New("only car files version 1 supported at present")
			}

			// duplicate of a block from above: in case we were streaming and never pre-read the file
			if keepTrackOfRoots {
				for _, cid := range car.Header.Roots {
					if _, exists := expectRoots[cid]; !exists {
						expectRoots[cid] = nil
					}
				}
			}

			for {
				block, err := car.Next()
				if err != nil && err != io.EOF {
					return err
				} else if block == nil {
					break
				}

				nd, err := ipld.Decode(block)
				if err != nil {
					return err
				}

				if err := b.Add(req.Context, nd); err != nil {
					return err
				}

				if keepTrackOfRoots {
					val, exists := expectRoots[nd.Cid()]

					// encountered something known to be a root, for the first time
					if exists && val == nil {
						expectRoots[nd.Cid()] = &nd
						if !silent {
							if err := res.Emit(&OutputObject{Cid: nd.Cid()}); err != nil {
								return err
							}
						}
					}
				}
			}
		}

		if err := it.Err(); err != nil {
			return err
		}

		if doPinRoots && len(expectRoots) > 0 {
			// run through everything making sure we found all roots
			toPin := make([]ipld.Node, 0, len(expectRoots))
			for cid, nd := range expectRoots {
				if nd == nil {
					return fmt.Errorf("the CID '%s' named as root, but never encountered in any .car", cid)
				}
				toPin = append(toPin, *nd)
			}

			// all found - pin them down
			// ( this does in effect call b.Commit()... it seems )
			if err := api.Dag().Pinning().AddMany(req.Context, toPin); err != nil {
				return err
			}
		}

		// well... here goes nothing
		if err := b.Commit(); err != nil {
			return err
		}

		return nil
	},
	Encoders: cmds.EncoderMap{
		cmds.Text: cmds.MakeTypedEncoder(func(req *cmds.Request, w io.Writer, out *OutputObject) error {
			enc, err := cmdenv.GetLowLevelCidEncoder(req)
			if err != nil {
				return err
			}
			fmt.Fprintln(w, enc.Encode(out.Cid))
			return nil
		}),
	},
}
