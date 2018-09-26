package org.sunbird.cloud.storage.util

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Days
import org.joda.time.LocalDate
import org.joda.time.Weeks
import org.joda.time.Years
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import java.util.Date
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.Paths.get
import java.nio.file.StandardCopyOption
import java.io.{File, FileInputStream, FileOutputStream, IOException}
import java.util.zip.{ZipEntry, ZipInputStream}

import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang3.StringUtils

object CommonUtil {

    @transient val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC();

    def getStartDate(endDate: Option[String], delta: Int): Option[String] = {
        val to = if (endDate.nonEmpty) dateFormat.parseLocalDate(endDate.get) else LocalDate.fromDateFields(new Date);
        Option(to.minusDays(delta).toString());
    }

    def getDatesBetween(fromDate: String, toDate: Option[String], pattern: String): Array[String] = {
        val df: DateTimeFormatter = DateTimeFormat.forPattern(pattern).withZoneUTC();
        val to = if (toDate.nonEmpty) df.parseLocalDate(toDate.get) else LocalDate.fromDateFields(new Date);
        val from = df.parseLocalDate(fromDate);
        val dates = datesBetween(from, to);
        dates.map { x => df.print(x) }.toArray;
    }

    def getDatesBetween(fromDate: String, toDate: Option[String]): Array[String] = {
        getDatesBetween(fromDate, toDate, "yyyy-MM-dd");
    }

    def datesBetween(from: LocalDate, to: LocalDate): IndexedSeq[LocalDate] = {
        val numberOfDays = Days.daysBetween(from, to).getDays()
        for (f <- 0 to numberOfDays) yield from.plusDays(f)
    }

    def createDirectory(dir: String) {
        val path = get(dir);
        Files.createDirectories(path);
    }

    def copyFile(from: InputStream, path: String, fileName: String) = {
        createDirectory(path);
        Files.copy(from, Paths.get(path + fileName), StandardCopyOption.REPLACE_EXISTING);
    }

    def unZip(zipFile: String, outputFolder: String): Unit = {
        val buffer = new Array[Byte](1024)
        try {
            //output directory
            val folder = new File(outputFolder);
            if (!folder.exists()) {
                folder.mkdir();
            }
            //zip file content
            val zis: ZipInputStream = new ZipInputStream(new FileInputStream(zipFile));
            //get the zipped file list entry
            var ze: ZipEntry = zis.getNextEntry();

            while (ze != null) {
                val fileName = ze.getName();
                if (!(FilenameUtils.getName(fileName).startsWith(".") | fileName.startsWith("_") | StringUtils.isBlank(FilenameUtils.getName(fileName)))) {
                    val newFile = new File(outputFolder + File.separator + fileName);
                    //create folders
                    new File(newFile.getParent()).mkdirs();
                    val fos = new FileOutputStream(newFile);
                    var len: Int = zis.read(buffer);
                    while (len > 0) {
                        fos.write(buffer, 0, len)
                        len = zis.read(buffer)
                    }
                    fos.close()
                }
                ze = zis.getNextEntry()
            }
            zis.closeEntry()
            zis.close()

        } catch {
            case e: IOException =>
                e.printStackTrace()
                println("exception caught: " + e.getMessage)
        }

    }
}
