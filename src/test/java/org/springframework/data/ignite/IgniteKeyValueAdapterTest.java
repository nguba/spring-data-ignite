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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public class IgniteKeyValueAdapterTest {

	private static final Integer ID = Integer.valueOf(Integer.MAX_VALUE);
	private static Ignite ignite;
	private static final String KEYSPACE = "Cache Name";

	@AfterClass
	public static void afterClass() {
		if (ignite != null) {
			ignite.close();
		}
	}

	private static final void assertKeyspaceHas(final String keyspace, final Serializable key, final Object data) {
		assertThat(ignite.cache(keyspace).get(key)).isEqualTo(data);
	}

	private static final void insert(Object value) {
		ignite.getOrCreateCache(KEYSPACE).put(ID, value);
	}

	@BeforeClass
	public static void setUpBeforeClass() {
		ignite = Ignition.start();
	}

	private IgniteKeyValueAdapter adapter;

	@Test
	public void contains_ShouldFindItem() {

		insert("Worthless values");
		assertThat(adapter.contains(ID, KEYSPACE)).isTrue();
	}

	@Test(expected = IllegalArgumentException.class)
	public void contains_ShouldNotAcceptNullKey() {

		adapter.contains(null, KEYSPACE);
	}

	@Test(expected = IllegalArgumentException.class)
	public void contains_ShouldNotAcceptNullKeyspace() {

		adapter.contains(ID, null);
	}

	@Test
	public void contains_ShouldRespectKeyspace() {

		insert("Worthless values");
		assertThat(adapter.contains(ID, "other keyspace")).isFalse();
	}

	@Test
	public void get_ShouldReturnItem() {
		insert("My Data");

		Object actual = adapter.get(ID, KEYSPACE);
		assertThat(actual).isEqualTo("My Data");
	}

	@Test
	public void get_ShouldReturnNullOnWrongKeyspace() {
		insert("My Data");

		Object actual = adapter.get(ID, KEYSPACE + "-other");
		assertThat(actual).isNull();
	}

	@Test
	public void get_ShouldReturnNullWhenNotFound() {
		insert("My Data");

		Object actual = adapter.get(Integer.MIN_VALUE, KEYSPACE);
		assertThat(actual).isNull();
	}

	@Test
	public void put_IsKeyspaceAware() {

		adapter.put(ID, "data 1", "K1");
		adapter.put(ID, "data 2", "K2");

		assertKeyspaceHas("K1", ID, "data 1");
		assertKeyspaceHas("K2", ID, "data 2");
	}

	@Test
	public void put_overwritesNewItem() {

		adapter.put(ID, "old data", KEYSPACE);
		final Object oldObject = adapter.put(ID, "new freeform data", KEYSPACE);
		assertThat(oldObject).isEqualTo("old data");

		assertKeyspaceHas(KEYSPACE, ID, "new freeform data");
	}

	@Test(expected = IllegalArgumentException.class)
	public void get_ShouldNotAcceptNullKey() {

		adapter.get(null, KEYSPACE);
	}

	@Test(expected = IllegalArgumentException.class)
	public void get_ShouldNotAcceptNullKeyspace() {

		adapter.get(ID, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void put_ShouldNotAcceptNullKey() {

		adapter.put(null, "", KEYSPACE);
	}

	@Test(expected = IllegalArgumentException.class)
	public void put_ShouldNotAcceptNullKeyspace() {

		adapter.put(ID, "", null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void delete_ShouldNotAcceptNullKey() {

		adapter.delete(null, KEYSPACE);
	}

	@Test(expected = IllegalArgumentException.class)
	public void delete_ShouldNotAcceptNullKeyspace() {

		adapter.delete(ID, null);
	}

	@Test
	public void delete_ReturnsRemovedObject() {

		insert("Mr Magoo");
		Object actual = adapter.delete(ID, KEYSPACE);
		assertThat(actual).asString().isEqualTo("Mr Magoo");
	}

	@Test
	public void delete_ReturnsNullOnNotFound() {

		Object actual = adapter.delete(ID, KEYSPACE);
		assertThat(actual).isNull();
	}

	@Test
	public void put_shouldReturnCorrectData() {

		final Object oldObject = adapter.put(ID, "some freeform data", KEYSPACE);
		assertThat(oldObject).isNull();

		assertKeyspaceHas(KEYSPACE, ID, "some freeform data");
	}

	@Test(expected = IllegalArgumentException.class)
	public void getAllOf_ShouldNotAcceptNullKeyspace() {

		adapter.getAllOf(null);
	}

	@Test
	public void getAllOf_HasExpectedItems() {
		List<Integer> expected = populateCacheItems();

		List<Integer> actual = new LinkedList<>();
		adapter.getAllOf(KEYSPACE).forEach(entry -> {
			@SuppressWarnings("unchecked")
			CacheEntryImpl<Integer, String> cacheEntry = ((CacheEntryImpl<Integer, String>) entry);
			actual.add(cacheEntry.getKey());
		});

		assertThat(actual).asList().isEqualTo(expected);
	}

	@Test
	public void deleteAllOf_clearsAllItems() {
		List<Integer> expected = populateCacheItems();
		
		assertThat(ignite.cache(KEYSPACE).size(CachePeekMode.PRIMARY)).isEqualTo(expected.size());
	
		adapter.deleteAllOf(KEYSPACE);
		
		assertThat(ignite.cache(KEYSPACE).size(CachePeekMode.PRIMARY)).isEqualTo(0);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void deleteAllOf_ShouldNotAcceptNullKeyspace() {

		adapter.deleteAllOf(null);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void count_ShouldNotAcceptNullKeyspace() {
		adapter.count(null);
	}
	
	@Test
	public void count_ReturnsExpectedValue() {
		
		List<Integer> expected = populateCacheItems();
		
		adapter.count(KEYSPACE);
		
		assertThat(ignite.cache(KEYSPACE).size(CachePeekMode.PRIMARY)).isEqualTo(expected.size());
	}
	
	@Test
	public void clear_ClearsAllCaches() {
	
		for(int i = 0; i < 10; i++) {
			adapter.put(ID, "data", String.format("Keyspace-%d", i));
		}
		
		for(String name : ignite.cacheNames()){
			assertThat(ignite.cache(name).size(CachePeekMode.PRIMARY)).as(name).isEqualTo(1);
		}
		
		adapter.clear();
		
		for(String name : ignite.cacheNames()){
			assertThat(ignite.cache(name).size(CachePeekMode.PRIMARY)).isEqualTo(0);
		}	
	}
	
	private static final List<Integer> populateCacheItems() {
		List<Integer> expected = new LinkedList<>();
		for (int i = 0; i < 10; i++) {
			Integer key = Integer.valueOf(i);
			expected.add(key);
			ignite.getOrCreateCache(KEYSPACE).put(key, "a");
		}
		return expected;
	}

	@Before
	public void setUp() throws Exception {
		adapter = new IgniteKeyValueAdapter(ignite);
	}

	@After
	public void tearDown() throws Exception {
		ignite.cacheNames().forEach(cache -> {
			ignite.cache(cache).destroy();
		});
	}
}
