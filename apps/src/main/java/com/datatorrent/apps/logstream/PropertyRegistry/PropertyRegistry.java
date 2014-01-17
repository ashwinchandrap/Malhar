/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.logstream.PropertyRegistry;

/**
 *
 * @param <T>
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public interface PropertyRegistry<T>
{
  public int bind(T name, T value);

  public int getIndex(T name, T value);

  public T lookupName(int register);

  public T lookupValue(int register);

  public T[] list(String name);
}
