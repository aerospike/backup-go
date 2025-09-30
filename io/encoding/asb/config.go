package asb

var (
	// versionDefault is the default version of the ASB format.
	versionDefault = newVersion(3, 1)
	// versionExpSindex is the version of the ASB format with expression Sindex support.
	// Should be used only for metadata files.
	versionExpSindex = newVersion(3, 2)
)

// EncoderConfig contains configuration options for the Encoder.
type EncoderConfig struct {
	// Namespace is the namespace to backup.
	Namespace string
	// Do not apply base-64 encoding to BLOBs: Bytes, HLL, RawMap, RawList.
	Compact bool
	// HasExpressionSindex indicates whether the backup contains an expression SIndex.
	// In that case an asb version will be bumped.
	HasExpressionSindex bool
}

// NewEncoderConfig returns a new encoder EncoderConfig.
func NewEncoderConfig(namespace string, compact, hasExprSindex bool) *EncoderConfig {
	return &EncoderConfig{
		Namespace:           namespace,
		Compact:             compact,
		HasExpressionSindex: hasExprSindex,
	}
}

// getVersion resolves version depending on the config.
func (c *EncoderConfig) getVersion() *version {
	if c.HasExpressionSindex {
		return versionExpSindex
	}

	return versionDefault
}
