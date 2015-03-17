/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.pi;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class TypeFinder
{
  public static Class getType(String type) {
    if(type.equals("int")) {
      return int.class;
    } else {
      try {
        return Class.forName(type);
      }
      catch (ClassNotFoundException ex) {
        throw new RuntimeException(ex);
      }
    }
  }
}
