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
package org.springframework.data.ignite;

import org.springframework.data.keyvalue.core.AbstractKeyValueAdapter;
import org.springframework.data.keyvalue.core.ForwardingCloseableIterator;
import org.springframework.data.util.CloseableIterator;
import org.springframework.util.Assert;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map.Entry;

public class IgniteKeyValueAdapter extends AbstractKeyValueAdapter {

	private final Ignite ignite;

	public IgniteKeyValueAdapter(final Ignite ignite) {
		super(null);
		Assert.notNull(ignite, "Ignite must not be 'null'.");
		this.ignite = ignite;
	}

	/*
	 * @see org.springframework.data.keyvalue.core.KeyValueAdapter#put(java.io.
	 * Serializable, java.lang.Object, java.io.Serializable)
	 */
	@Override
	public Object put(final Serializable id, final Object item, final Serializable keyspace) {

		Assert.notNull(id, "id must not be 'null'.");
		return getCache(keyspace).getAndPut(id, item);
	}

	private final IgniteCache<Object, Object> getCache(final Serializable keyspace) {

		Assert.isInstanceOf(String.class, keyspace, "Keyspace identifier must of type String.");
		return ignite.getOrCreateCache((String) keyspace);
	}

	/*
	 * @see
	 * org.springframework.data.keyvalue.core.KeyValueAdapter#contains(java.io.
	 * Serializable, java.io.Serializable)
	 */
	@Override
	public boolean contains(final Serializable id, final Serializable keyspace) {

		Assert.notNull(id, "id must not be 'null'.");
		return getCache(keyspace).containsKey(id);
	}

	/*
	 * @see org.springframework.data.keyvalue.core.KeyValueAdapter#get(java.io.
	 * Serializable, java.io.Serializable)
	 */
	@Override
	public Object get(final Serializable id, final Serializable keyspace) {

		Assert.notNull(id, "id must not be 'null'.");
		return getCache(keyspace).get(id);
	}

	/*
	 * @see
	 * org.springframework.data.keyvalue.core.KeyValueAdapter#delete(java.io.
	 * Serializable, java.io.Serializable)
	 */
	@Override
	public Object delete(final Serializable id, final Serializable keyspace) {

		Assert.notNull(id, "id must not be 'null'.");
		return getCache(keyspace).getAndRemove(id);
	}

	/*
	 * @see
	 * org.springframework.data.keyvalue.core.KeyValueAdapter#getAllOf(java.io.
	 * Serializable)
	 */
	@Override
	public Iterable<?> getAllOf(final Serializable keyspace) {
		return getCache(keyspace);
	}

	/*
	 * @see
	 * org.springframework.data.keyvalue.core.KeyValueAdapter#entries(java.io.
	 * Serializable)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public CloseableIterator<Entry<Serializable, Object>> entries(final Serializable keyspace) {
		return new ForwardingCloseableIterator<>(
				(Iterator<? extends Entry<Serializable, Object>>) getCache(keyspace).iterator());
	}

	/*
	 * @see
	 * org.springframework.data.keyvalue.core.KeyValueAdapter#deleteAllOf(java.
	 * io.Serializable)
	 */
	@Override
	public void deleteAllOf(final Serializable keyspace) {
		getCache(keyspace).clear();
	}

	/*
	 * @see org.springframework.data.keyvalue.core.KeyValueAdapter#clear()
	 */
	@Override
	public void clear() {
		ignite.cacheNames().forEach(cache -> {
			ignite.cache(cache).clear();
		});
	}

	/*
	 * @see
	 * org.springframework.data.keyvalue.core.KeyValueAdapter#count(java.io.
	 * Serializable)
	 */
	@Override
	public long count(final Serializable keyspace) {
		return getCache(keyspace).size(CachePeekMode.PRIMARY);
	}

	/*
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	@Override
	public void destroy() throws Exception {
		ignite.close();
	}

}
