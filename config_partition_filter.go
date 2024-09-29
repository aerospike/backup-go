package backup

import a "github.com/aerospike/aerospike-client-go/v7"

// NewPartitionFilterByRange returns a partition range with boundaries specified by the provided values.
func NewPartitionFilterByRange(begin, count int) *a.PartitionFilter {
	return a.NewPartitionFilterByRange(begin, count)
}

// NewPartitionFilterByID returns a partition filter by id with specified id.
func NewPartitionFilterByID(partitionID int) *a.PartitionFilter {
	return a.NewPartitionFilterById(partitionID)
}

// NewPartitionFilterByDigest returns a partition filter by digest with specified value.
func NewPartitionFilterByDigest(namespace, digest string) (*a.PartitionFilter, error) {
	key, err := newKeyByDigest(namespace, digest)
	if err != nil {
		return nil, err
	}

	return a.NewPartitionFilterByKey(key), nil
}

// NewPartitionFilterAfterDigest returns partition filter to scan call records after digest.
func NewPartitionFilterAfterDigest(namespace, digest string) (*a.PartitionFilter, error) {
	key, err := newKeyByDigest(namespace, digest)
	if err != nil {
		return nil, err
	}

	defaultFilter := NewPartitionFilterAll()
	begin := key.PartitionId()
	count := defaultFilter.Count - begin

	return &a.PartitionFilter{
		Begin:  begin,
		Count:  count,
		Digest: key.Digest(),
	}, nil
}

// NewPartitionFilterAll returns a partition range containing all partitions.
func NewPartitionFilterAll() *a.PartitionFilter {
	return a.NewPartitionFilterByRange(0, MaxPartitions)
}
