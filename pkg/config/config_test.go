/*
 * Copyright 2026 InfAI (CC SES)
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

package config

import (
	"bytes"
	"log"
	"strings"
	"testing"
)

func TestHandleEnvironmentVarsMasksSecrets(t *testing.T) {
	t.Setenv("MQTT_PW", "the-password")
	t.Setenv("MQTT_USER", "the-client")
	config := &Config{}

	var buf bytes.Buffer
	orig := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(orig)
	handleEnvironmentVars(config)
	out := buf.String()

	if strings.Contains(out, "the-password") {
		t.Errorf("mqtt password logged: %q", out)
	}
	if !strings.Contains(out, "the-client") {
		t.Errorf("other values not logged: %q", out)
	}
	if config.MqttPw != "the-password" {
		t.Errorf("mqtt password not applied: %q", config.MqttPw)
	}
}
