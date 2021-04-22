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

package mqtt

import (
	"context"
	"errors"
	paho "github.com/eclipse/paho.mqtt.golang"
	uuid "github.com/satori/go.uuid"
	"log"
	"sync"
)

type Publisher struct {
	client paho.Client
}

func NewPublisher(ctx context.Context, wg *sync.WaitGroup, broker string, user string, pw string, client string) (mqtt *Publisher, err error) {
	mqtt = &Publisher{}
	options := paho.NewClientOptions().
		SetPassword(pw).
		SetUsername(user).
		SetAutoReconnect(true).
		SetCleanSession(true).
		SetClientID(client + "_" + uuid.NewV4().String()).
		AddBroker(broker)

	mqtt.client = paho.NewClient(options)
	if token := mqtt.client.Connect(); token.Wait() && token.Error() != nil {
		log.Println("Error on Publisher.Connect(): ", broker, user, pw, client, token.Error())
		return mqtt, token.Error()
	}
	log.Println("MQTT publisher up and running...")
	wg.Add(1)
	go func() {
		<-ctx.Done()
		mqtt.client.Disconnect(0)
		wg.Done()
	}()

	return mqtt, nil
}

func (this *Publisher) Publish(topic string, msg string) (err error) {
	if !this.client.IsConnected() {
		return errors.New("mqtt client not connected")
	}
	token := this.client.Publish(topic, 2, false, msg)
	if token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return err
}
