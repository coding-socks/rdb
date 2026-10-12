package crc64jones

import (
	"hash/crc64"
	"testing"
)

func Test_Checksum(t *testing.T) {
	type args struct {
		data []byte
		tab  *crc64.Table
	}
	tests := []struct {
		name string
		args args
		want uint64
	}{
		{
			name: "123456789",
			args: args{data: []byte("123456789"), tab: crc64.MakeTable(Jones)},
			want: 0xe9c6d914c4b8d9ca,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Checksum(tt.args.data, tt.args.tab); got != tt.want {
				t.Errorf("Checksum() = %v, want %v", got, tt.want)
			}
		})
	}
}
