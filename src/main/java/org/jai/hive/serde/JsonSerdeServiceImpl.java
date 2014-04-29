package org.jai.hive.serde;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

public class JsonSerdeServiceImpl implements JsonSerdeService {

	@Override
	public String getJsonJarPath() {
		return createJsonSerdeJarFile();
	}
	
	private String createJsonSerdeJarFile() {
		try {
			Manifest manifest = new Manifest();
			manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION,
					"1.0");
			File jarFile = new File("jaihivejsonserde-1.0.jar");
			FileOutputStream fileOutputStream = new FileOutputStream(
					jarFile);
			JarOutputStream target = new JarOutputStream(fileOutputStream, manifest);
			addFile(new File("./target/classes/"), target, JsonSerde.class);
			target.close();
			return jarFile.getAbsolutePath();
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Error creating json serde jar file!",e);
		}
	}
	
	private void addFile(File source, JarOutputStream target, Class clazz) throws IOException
	{
	  BufferedInputStream in = null;
	  try
	  {
	    if (source.isDirectory())
	    {
	      String name = source.getPath().replace("\\", "/");
	      if (!name.isEmpty())
	      {
	        if (!name.endsWith("/"))
	          name += "/";
	        JarEntry entry = new JarEntry(name);
	        entry.setTime(source.lastModified());
	        target.putNextEntry(entry);
	        target.closeEntry();
	      }
	      for (File nestedFile: source.listFiles())
	    	  addFile(nestedFile, target, clazz);
	      return;
	    }

	    JarEntry entry = new JarEntry(source.getPath().replace("\\", "/"));
	    entry.setTime(source.lastModified());
	    target.putNextEntry(entry);
	    in = new BufferedInputStream(new FileInputStream(source));

	    byte[] buffer = new byte[1024];
	    while (true)
	    {
	      int count = in.read(buffer);
	      if (count == -1)
	        break;
	      target.write(buffer, 0, count);
	    }
	    target.closeEntry();
	  }
	  finally
	  {
	    if (in != null)
	      in.close();
	  }
	}
}
