//go:build test
// +build test

package asb

import (
	"testing"

	"github.com/aerospike/backup-go/encoding"
)

func Test_verifyBackupFileExtension(t *testing.T) {
	type args struct {
		decoder  encoding.DecoderFactory
		fileName string
	}
	tests := []struct {
		args    args
		name    string
		wantErr bool
	}{
		{
			name: "Positive ASB",
			args: args{
				fileName: "file1.asb",
				decoder:  NewASBDecoderFactory(),
			},
		},
		{
			name: "Negative ASB",
			args: args{
				fileName: "file1.txt",
				decoder:  NewASBDecoderFactory(),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.args.decoder.Validate(tt.args.fileName); (err != nil) != tt.wantErr {
				t.Errorf("verifyBackupFileExtension() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
