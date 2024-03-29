/*
 * Copyright 2021 InfAI (CC SES)
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

package lib

import (
	"github.com/itchyny/gojq"
	"strings"
	"unicode"
)

type PathTopicMapping struct {
	Query *gojq.Code
	Topic string
}

type PathTopicMappingString struct {
	Query string
	Topic string
}

func (mapping *PathTopicMappingString) ToMapping() (PathTopicMapping, error) {
	queryString := ""
	queryParts := strings.Split(mapping.Query, ".")
	for _, queryPart := range queryParts {
		if len(queryPart) == 0 {
			continue
		}
		if unicode.IsDigit(rune(queryPart[0])) {
			queryString += "[\"" + queryPart + "\"]"
		} else {
			queryString += "." + queryPart
		}
	}
	query, err := gojq.Parse(queryString)
	if err != nil {
		return PathTopicMapping{}, err
	}
	code, err := gojq.Compile(query)
	if err != nil {
		return PathTopicMapping{}, err
	}
	return PathTopicMapping{
		Query: code,
		Topic: mapping.Topic,
	}, nil
}
