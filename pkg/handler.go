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
	"github.com/SENERGY-Platform/kafka2mqtt/pkg/lib/kafka"
	"github.com/SENERGY-Platform/kafka2mqtt/pkg/lib/mqtt"
	"github.com/yasuyuky/jsonpath"
	"log"
	"time"
)

type Handler struct{
	Publisher *mqtt.Publisher
	MqttTopic string
	FilterPath []interface{}
	FilterValue interface{}
}

func(this *Handler) handleMessage(topic string, msg []byte, time time.Time) error {
	messageString := string(msg)
	if this.FilterPath != nil && this.FilterValue != nil {
		data, err := jsonpath.DecodeString(messageString)
		if err != nil {
			return err
		}
		v, err := jsonpath.Get(data, this.FilterPath,nil)
		if err != nil || v == nil || v != this.FilterValue {
			return nil
		}
	}
	err := this.Publisher.Publish(this.MqttTopic, messageString)
	return err
}

func handleError(err error, consumer *kafka.Consumer) {
	log.Println(err)
}
