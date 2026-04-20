package local

// noopWriter discards all writes and closes successfully.
type noopWriter struct{}

func (noopWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func (noopWriter) Close() error {
	return nil
}
