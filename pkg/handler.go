/*
 * Copyright 2020 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pkg

import (
	"encoding/json"
	"errors"
	"github.com/SENERGY-Platform/kafka2mqtt/pkg/lib"
	"github.com/SENERGY-Platform/kafka2mqtt/pkg/lib/kafka"
	"github.com/SENERGY-Platform/kafka2mqtt/pkg/lib/mqtt"
	"github.com/itchyny/gojq"
	"log"
	"time"
)

type Handler struct {
	Publisher   *mqtt.Publisher
	Mappings    []lib.PathTopicMapping
	FilterQuery *gojq.Query
}

func NewHandler(publisher *mqtt.Publisher, mappings []lib.PathTopicMappingString, FilterQuery *string) (handler Handler, err error) {
	transformedMappings := make([]lib.PathTopicMapping, len(mappings))
	for i := range mappings {
		transformedMappings[i], err = mappings[i].ToMapping()
		if err != nil {
			return
		}
	}
	handler.Publisher = publisher
	handler.Mappings = transformedMappings
	if FilterQuery != nil {
		handler.FilterQuery, err = gojq.Parse(*FilterQuery)
	}
	return
}

func (this *Handler) handleMessage(_ string, msg []byte, _ time.Time) error {
	var data interface{}
	err := json.Unmarshal(msg, &data)
	if err != nil {
		return err
	}
	if this.FilterQuery != nil {
		iter := this.FilterQuery.Run(data)
		found := false
		for {
			v, ok := iter.Next()
			if !ok {
				break
			}
			if err, ok := v.(error); ok {
				return err
			}
			vBool, ok := v.(bool)
			if !ok {
				return errors.New("filterQuery returned on bool value")
			}
			if vBool {
				found = true
			}
		}
		if !found {
			return nil
		}
	}
	for i := range this.Mappings {
		iter := this.Mappings[i].Query.Run(data)
		for {
			v, ok := iter.Next()
			if !ok {
				break
			}
			if err, ok := v.(error); ok {
				return err
			}

			var mqttMsg string
			mqttMsg, isString := v.(string)
			if !isString {
				bs, err := json.Marshal(v)
				if err != nil {
					return err
				}
				mqttMsg = string(bs)
			}
			err = this.Publisher.Publish(this.Mappings[i].Topic, mqttMsg)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func handleError(err error, _ *kafka.Consumer) {
	log.Println(err)
}
