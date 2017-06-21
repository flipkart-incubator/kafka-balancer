package com.flipkart.fdpinfra.kafka.balancer.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import scala.collection.Seq;

public class ScalaConversions {
	public static <T> List<T> scalaSeqToJavaList(Seq<T> scalaSeq) {
		return scala.collection.JavaConversions.asJavaList(scalaSeq);
	}

	public static <K, V> Map<K, V> scalaMapToJavaMap(scala.collection.Map<K, V> scalaMap) {
		return scala.collection.JavaConversions.asJavaMap(scalaMap);
	}

	public static <T> Seq<T> javaCollectionToScalaSeq(Collection<T> javaCollection) {
		List<T> list = new ArrayList<>(javaCollection);
		return scala.collection.JavaConversions.asScalaBuffer(list).seq();
	}

}
