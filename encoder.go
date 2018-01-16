// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kstreams

// Encoder provides a simple interface to allow any type to be encoded
// as an array of bytes.
type Encoder interface {
	// Encode the content to a byte array suitable as a Kafka key or value
	Encode() ([]byte, error)
}

// StringEncoder provides Encoder wrapper for strings
type StringEncoder string

// Encode satisfies the Encoder interface
func (s StringEncoder) Encode() ([]byte, error) {
	return []byte(s), nil
}

// ByteEncoder provides Encoder wrapper for []byte
type ByteEncoder []byte

// Encode satisfies the Encoder interface
func (b ByteEncoder) Encode() ([]byte, error) {
	return b, nil
}
