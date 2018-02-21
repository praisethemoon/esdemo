package esdemo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.pdf.PDFParser;
import org.apache.tika.sax.BodyContentHandler;
import org.json.simple.JSONObject;
import org.xml.sax.SAXException;

public class DocumentParserFactory {
	
	/*
	 * Transforms document's metadata to JSON Object
	 */
	public static JSONObject parse(File file) throws TikaException, IOException, SAXException{
		JSONObject obj = new JSONObject();
		Map<String, String> map = new HashMap<String, String>();
		
		InputStream input = new FileInputStream(file);
		BodyContentHandler handler = new BodyContentHandler(Integer.MAX_VALUE);
        Metadata metadata = new Metadata();
        new PDFParser().parse(input, handler, metadata, new ParseContext());
        String plainText = handler.toString();
        String metadataNames[] = metadata.names();
        
        for(final String name: metadataNames){
        	obj.put(name, metadata.get(name));
        }
        
        obj.put("name", file.getName());
        // TODO change _size to _len
        obj.put("len", file.length());
        obj.put("content", plainText);
        
        return obj;
	}
}
