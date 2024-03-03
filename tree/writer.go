package tree

// // Writer writes messages to a tree.
//type Writer struct {
//	ns  *nodestore.Nodestore
//	buf *bytes.Buffer
//	w   *fmcap.Writer
//
//	lower uint64
//	upper uint64
//
//	schemas     []*fmcap.Schema
//	channels    []*fmcap.Channel
//	initialized bool
//}
//
//// NewWriter returns a new writer for the given tree.
//func NewWriter(
//	ns *nodestore.Nodestore,
//	rootID nodestore.NodeID,
//	start uint64,
//	end uint64,
//	leafWidth time.Duration,
//	branchingFactor int,
//) (*Writer, error) {
//	buf := &bytes.Buffer{}
//	return &Writer{
//		ns:          ns,
//		buf:         buf,
//		schemas:     []*fmcap.Schema{},
//		channels:    []*fmcap.Channel{},
//		initialized: false,
//	}, nil
//}
//
//// Close closes the writer.
//func (w *Writer) Close() error {
//	if err := w.flush(); err != nil {
//		return fmt.Errorf("failed to flush mcap writer: %w", err)
//	}
//	return nil
//}
//
//func (w *Writer) flush(rootID nodestore.NodeID, version uint64) error {
//	if err := w.w.Close(); err != nil {
//		return fmt.Errorf("failed to close mcap writer: %w", err)
//	}
//	if rootID, _, err := Insert(w.ns, rootID, version, w.lower*1e9, w.buf.Bytes()); err != nil {
//		return fmt.Errorf("failed to insert leaf at time %d: %w", w.lower, err)
//	}
//	w.buf.Reset()
//	w.initialized = false
//	return nil
//}
//
//func (w *Writer) initialize(ts uint64) error {
//	bounds, err := w.t.Bounds(ts)
//	if err != nil {
//		return fmt.Errorf("failed to get bounds: %w", err)
//	}
//	w.w, err = mcap.NewWriter(w.buf)
//	if err != nil {
//		return fmt.Errorf("failed to create mcap writer: %w", err)
//	}
//	if err = w.w.WriteHeader(&fmcap.Header{}); err != nil {
//		return fmt.Errorf("failed to write header: %w", err)
//	}
//	w.lower = bounds[0]
//	w.upper = bounds[1]
//	for _, schema := range w.schemas {
//		if err := w.w.WriteSchema(schema); err != nil {
//			return fmt.Errorf("failed to write schema: %w", err)
//		}
//	}
//	for _, channel := range w.channels {
//		if err := w.w.WriteChannel(channel); err != nil {
//			return fmt.Errorf("failed to write channel: %w", err)
//		}
//	}
//	w.initialized = true
//	return nil
//}
//
//func (w *Writer) reset(ts uint64) error {
//	if err := w.flush(); err != nil {
//		return fmt.Errorf("failed to flush writer: %w", err)
//	}
//	if err := w.initialize(ts); err != nil {
//		return fmt.Errorf("failed to initialize writer on reset: %w", err)
//	}
//	return nil
//}
//
//// WriteSchema writes the given schema to the tree.
//func (w *Writer) WriteSchema(schema *fmcap.Schema) error {
//	known := slices.Contains(w.schemas, schema)
//	if w.initialized && known {
//		return nil
//	}
//	if w.initialized && !known {
//		if err := w.w.WriteSchema(schema); err != nil {
//			return fmt.Errorf("failed to write schema: %w", err)
//		}
//	}
//	if !known {
//		w.schemas = append(w.schemas, schema)
//	}
//	return nil
//}
//
//// WriteChannel writes the given channel to the tree.
//func (w *Writer) WriteChannel(channel *fmcap.Channel) error {
//	known := slices.Contains(w.channels, channel)
//	if w.initialized && known {
//		return nil
//	}
//	if w.initialized && !known {
//		if err := w.w.WriteChannel(channel); err != nil {
//			return fmt.Errorf("failed to write channel: %w", err)
//		}
//	}
//	if !known {
//		w.channels = append(w.channels, channel)
//	}
//	return nil
//}
//
//// WriteMessage writes the given message to the tree.
//func (w *Writer) WriteMessage(message *fmcap.Message) error {
//	if !w.initialized {
//		if err := w.initialize(message.LogTime); err != nil {
//			return fmt.Errorf("failed to initialize writer: %w", err)
//		}
//	}
//	if message.LogTime < w.lower*1e9 || message.LogTime >= w.upper*1e9 {
//		if err := w.reset(message.LogTime); err != nil {
//			return fmt.Errorf("failed to reset writer: %w", err)
//		}
//	}
//	if err := w.w.WriteMessage(message); err != nil {
//		return fmt.Errorf("failed to write message: %w", err)
//	}
//	return nil
//}
//
