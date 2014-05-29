/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.io;

import java.net.URI;
import javax.validation.constraints.NotNull;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
@Deprecated
public class HttpOutputOperator<T> extends HttpPostOutputOperator<T>
{
  @Deprecated
  public void setResourceURL(URI url)
  {
    if (!url.isAbsolute()) {
      throw new IllegalArgumentException("URL is not absolute: " + url);
    }

    this.url = url.toString();
  }

}
