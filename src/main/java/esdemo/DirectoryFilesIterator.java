package esdemo;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;

public class DirectoryFilesIterator implements Iterable<File>, Iterator<File> {
	File directory;
	ArrayList<File> listOfFiles;
	int counter;
	
	public DirectoryFilesIterator(String dir){
		directory = new File(dir);
		File[] list = directory.listFiles();
		listOfFiles = new ArrayList<File>();
		for(final File f: list){
			if(f.getName().indexOf(".pdf") > 0){
				listOfFiles.add(f);
			}
		}
		
		counter = 0;
	}

	public boolean hasNext() {
		return counter < listOfFiles.size();
	}

	public File next() {
		return listOfFiles.get(counter++);
	}
	
	public int length(){
		return listOfFiles.size();
	}
	
	public void reset(){
		counter = 0;
	}

	public Iterator<File> iterator() {
		return this;
	}
}
