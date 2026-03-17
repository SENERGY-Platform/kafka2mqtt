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
	"context"
	"encoding/json"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/SENERGY-Platform/go-service-base/struct-logger/attributes"
	"github.com/SENERGY-Platform/kafka2mqtt/pkg/config"
	"github.com/SENERGY-Platform/kafka2mqtt/pkg/lib"
	"github.com/SENERGY-Platform/kafka2mqtt/pkg/lib/kafka"
	"github.com/SENERGY-Platform/kafka2mqtt/pkg/lib/mqtt"
	"github.com/SENERGY-Platform/kafka2mqtt/pkg/log"
)

func Start(ctx context.Context, config config.Config) (wg *sync.WaitGroup, err error) {
	wg = &sync.WaitGroup{}
	broker := config.MqttBroker
	if !strings.Contains(broker, ":") {
		broker += ":1883"
	}
	publisher, err := mqtt.NewPublisher(ctx, wg, broker, config.MqttUser, config.MqttPw, config.MqttClientId, config.MqttQos, config.Debug)
	if err != nil {
		debug.PrintStack()
		return wg, err
	}
	var mappings []lib.PathTopicMappingString
	err = json.Unmarshal([]byte(config.MqttTopicMapping), &mappings)
	if err != nil {
		debug.PrintStack()
		return wg, err
	}

	filterQuery := &config.FilterQuery

	if len(config.FilterQuery) == 0 {
		filterQuery = nil
	}

	handler, err := NewHandler(publisher, mappings, filterQuery)
	if err != nil {
		debug.PrintStack()
		return wg, err
	}

	var offset int64
	switch config.KafkaOffset {
	case "latest", "largest":
		offset = kafka.Latest
		break
	case "earliest", "smallest":
		offset = kafka.Earliest
		break
	default:
		log.Logger.Warn("unknown kafka offset, using latest", "offset", config.KafkaOffset)
		offset = kafka.Latest
	}
	_, err = kafka.NewConsumer(ctx, wg, config.KafkaBootstrap, []string{config.KafkaTopic}, config.KafkaGroupId, offset, handler.handleMessage, handleError, config.Debug)
	if err != nil {
		log.Logger.Error("unable to start kafka connection", attributes.ErrorKey, err)
		return wg, err
	}

	return
}
