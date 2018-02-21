package esdemo;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tika.exception.TikaException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.xml.sax.SAXException;

public class ESClientExample {

	public static void main(String[] args) {
		final Log logger = LogFactory.getLog(ESClientExample.class);
		
		if(args.length < 1){
			logger.fatal("Invalid number of arguments. Please specify the directory from which to load pdf files.");
			return;
		}
		
		DirectoryFilesIterator iterator = new DirectoryFilesIterator(args[0]);
		JSONArray pdfs = new JSONArray();
		
		int successful = 0;
		int total = iterator.length();
		
		for(final File f: iterator){
			try {
				JSONObject obj = DocumentParserFactory.parse(f);
				String id = ThreadedElasticsearchHTTPService.send("library/books", obj);
				//String id = ThreadedElasticsearchHTTPService.sendAsBulk(obj);
				if(id != null)
					successful++;
			} catch (TikaException | IOException | SAXException e) {
				logger.fatal(e);
			}
		}
		
		System.out.println(String.format("Uploaded %d/%d files!", successful, total));
	}

}
