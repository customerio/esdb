/*
The blocks package is responsible for reading and writing potentially
compressed binary data in blocks or chunks.

It allows O(1) access to the content of any block if you know the offset of the
block in a sea of bytes.

The reader and writer objects implement many of the common `io` package
interfaces which should make them drop in replacements for many use cases where
you'd use `byte.Buffer`, `byte.Reader`, `bufio.Reader`, `bufio.Writer`, etc.
*/
package blocks

const (
	NO_COMPRESSION     = iota
	SNAPPY_COMPRESSION = iota
)
