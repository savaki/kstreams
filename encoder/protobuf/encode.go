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

package protobuf

import (
	"github.com/gogo/protobuf/proto"
)

// Encoder implements kstreams.Encoder and wraps a proto.Message
type Encoder struct {
	message proto.Message
}

// Encode implements kstreams.Encoder
func (p Encoder) Encode() ([]byte, error) {
	return proto.Marshal(p.message)
}

// Encode provides a convenient encoder for protobuf messages
func Encode(m proto.Message) Encoder {
	return Encoder{message: m}
}
