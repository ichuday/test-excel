/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Iterator;
import static java.nio.charset.StandardCharsets.UTF_8;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.StorageOptions;

/**
 * An example that counts words in Shakespeare.
 *
 * <p>
 * This class, {@link MinimalWordCount}, is the first in a series of four
 * successively more detailed 'word count' examples. Here, for simplicity, we
 * don't show any error-checking or argument processing, and focus on
 * construction of the pipeline, which chains together the application of core
 * transforms.
 *
 * <p>
 * Next, see the {@link WordCount} pipeline, then the
 * {@link DebuggingWordCount}, and finally the {@link WindowedWordCount}
 * pipeline, for more detailed examples that introduce additional concepts.
 *
 * <p>
 * Concepts:
 *
 * <pre>
 *   1. Reading data from text files
 *   2. Specifying 'inline' transforms
 *   3. Counting items in a PCollection
 *   4. Writing data to text files
 * </pre>
 *
 * <p>
 * No arguments are required to run this pipeline. It will be executed with the
 * DirectRunner. You can see the results in the output files in your current
 * working directory, with names like "wordcounts-00001-of-00005. When running
 * on a distributed service, you would use an appropriate file service.
 */
public class MinimalWordCount {
	private static final int BUFFER_SIZE = 64 * 1024;

	private static void printBlob(com.google.cloud.storage.Storage storage, String bucketName, String blobPath)
			throws IOException, InvalidFormatException {
		try (ReadChannel reader = ((com.google.cloud.storage.Storage) storage).reader(bucketName, blobPath)) {
			InputStream inputStream = Channels.newInputStream(reader);
			Workbook wb = WorkbookFactory.create(inputStream);
			StringBuffer data = new StringBuffer();
			for (int i = 0; i < wb.getNumberOfSheets(); i++) {
				String fName = wb.getSheetAt(i).getSheetName();
				File outputFile = new File("D:\\excel\\" + fName + ".csv");
				FileOutputStream fos = new FileOutputStream(outputFile);
				XSSFSheet sheet = (XSSFSheet) wb.getSheetAt(i);
				Iterator<Row> rowIterator = sheet.iterator();
				data.delete(0, data.length());
				while (rowIterator.hasNext()) {
					// Get Each Row
					Row row = rowIterator.next();
					data.append('\n');
					// Iterating through Each column of Each Row
					Iterator<Cell> cellIterator = row.cellIterator();

					while (cellIterator.hasNext()) {
						Cell cell = cellIterator.next();

						// Checking the cell format
						switch (cell.getCellType()) {
						case Cell.CELL_TYPE_NUMERIC:
							data.append(cell.getNumericCellValue() + ",");
							break;
						case Cell.CELL_TYPE_STRING:
							data.append(cell.getStringCellValue() + ",");
							break;
						case Cell.CELL_TYPE_BOOLEAN:
							data.append(cell.getBooleanCellValue() + ",");
							break;
						case Cell.CELL_TYPE_BLANK:
							data.append("" + ",");
							break;
						default:
							data.append(cell + ",");
						}
					}

				}
				String filename = "test_excel/"+fName;
				BlobId blobId = BlobId.of("adefficiency", filename);
				 byte[] content = data.toString().getBytes(UTF_8);
				BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
				try (WriteChannel writer = storage.writer(blobInfo)) {
					writer.write(ByteBuffer.wrap(content, 0, content.length));
				}
			}
		}
	}


	public static void main(String[] args) throws IOException {

		// Create a PipelineOptions object. This object lets us set various execution
		// options for our pipeline, such as the runner you wish to use. This example
		// will run with the DirectRunner by default, based on the class path configured
		// in its dependencies.
		DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
		options.setRunner(DataflowRunner.class);
		options.setProject("ad-efficiency-dev");
		options.setStagingLocation("gs://adefficiency/staging");
		DataflowRunner.fromOptions(options);
		// In order to run your pipeline, you need to make following runner specific
		// changes:
		//
		// CHANGE 1/3: Select a Beam runner, such as BlockingDataflowRunner
		// or FlinkRunner.
		// CHANGE 2/3: Specify runner-required options.
		// For BlockingDataflowRunner, set project and temp location as follows:
		// DataflowPipelineOptions dataflowOptions =
		// options.as(DataflowPipelineOptions.class);
		// dataflowOptions.setRunner(BlockingDataflowRunner.class);
		// dataflowOptions.setProject("SET_YOUR_PROJECT_ID_HERE");
		// dataflowOptions.setTempLocation("gs://SET_YOUR_BUCKET_NAME_HERE/AND_TEMP_DIRECTORY");
		// For FlinkRunner, set the runner as follows. See {@code FlinkPipelineOptions}
		// for more details.
		// options.as(FlinkPipelineOptions.class)
		// .setRunner(FlinkRunner.class);

		// Create the Pipeline object with the options we defined above
		Pipeline p = Pipeline.create(options);
		com.google.cloud.storage.Storage storage = StorageOptions.getDefaultInstance().getService();
		String bucketName = "adefficiency";
		String blobPath = "test_excel/MARKETS_duplicates.xlsx";

		try {
			printBlob(storage, bucketName, blobPath);
		} catch (InvalidFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// Concept #1: Apply a root transform to the pipeline; in this case, TextIO.Read
		// to read a set
		// of input text files. TextIO.Read returns a PCollection where each element is
		// one line from
		// the input text (a set of Shakespeare's texts).

		// This example reads a public data set consisting of the complete works of
		// Shakespeare.
		p.apply(TextIO.read().from("gs://adefficiency/test_excel/MARKETS_duplicates.xlsx"))

				// Concept #2: Apply a FlatMapElements transform the PCollection of text lines.
				// This transform splits the lines in PCollection<String>, where each element is
				// an
				// individual word in Shakespeare's collected texts.
				.apply(FlatMapElements.into(TypeDescriptors.strings())
						.via((String word) -> Arrays.asList(word.split("[^\\p{L}]+"))))
				// We use a Filter transform to avoid empty word
				.apply(Filter.by((String word) -> !word.isEmpty()))
				// Concept #3: Apply the Count transform to our PCollection of individual words.
				// The Count
				// transform returns a new PCollection of key/value pairs, where each key
				// represents a
				// unique word in the text. The associated value is the occurrence count for
				// that word.
				.apply(Count.perElement())
				// Apply a MapElements transform that formats our PCollection of word counts
				// into a
				// printable string, suitable for writing to an output file.
				.apply(MapElements.into(TypeDescriptors.strings())
						.via((KV<String, Long> wordCount) -> wordCount.getKey() + ": " + wordCount.getValue()))
				// Concept #4: Apply a write transform, TextIO.Write, at the end of the
				// pipeline.
				// TextIO.Write writes the contents of a PCollection (in this case, our
				// PCollection of
				// formatted strings) to a series of text files.
				//
				// By default, it will write to a set of files with names like
				// wordcounts-00001-of-00005
				.apply(TextIO.write().to("gs://adefficiency/test_excel/excel"));

		p.run().waitUntilFinish();
	}
}