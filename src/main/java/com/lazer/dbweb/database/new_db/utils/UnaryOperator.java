package com.lazer.dbweb.database.new_db.utils;

@FunctionalInterface
public interface UnaryOperator<T> {
    T apply(T t);
}
