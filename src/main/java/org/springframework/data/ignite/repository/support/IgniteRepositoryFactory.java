/*
 * Copyright 2014-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.ignite.repository.support;

import org.springframework.data.keyvalue.core.KeyValueOperations;
import org.springframework.data.keyvalue.repository.support.KeyValueRepositoryFactory;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;

public class IgniteRepositoryFactory extends KeyValueRepositoryFactory {

	/**
	 * @param keyValueOperations
	 * @param queryCreator
	 */
	public IgniteRepositoryFactory(KeyValueOperations keyValueOperations,
			Class<? extends AbstractQueryCreator<?, ?>> queryCreator) {
		super(keyValueOperations);
	}

}