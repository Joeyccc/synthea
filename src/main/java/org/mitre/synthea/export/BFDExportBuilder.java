package org.mitre.synthea.export;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;

import org.mitre.synthea.world.agents.Payer;
import org.mitre.synthea.world.agents.Person;
import org.mitre.synthea.world.agents.Provider.ProviderType;
import org.mitre.synthea.world.concepts.HealthRecord;
import org.mitre.synthea.world.concepts.HealthRecord.EncounterType;
import org.mitre.synthea.world.concepts.HealthRecord.Medication;



/** Export configuration class for BFD.
 */
public class BFDExportBuilder {

  enum ExportConfigType {
    BENEFICIARY,
    BENEFICIARY_HISTORY,
    CARRIER,
    // DME,
    INPATIENT,
    // HHA,
    // HOSPICE,
    // MEDICARE_BENEFICIARY_ID,
    OUTPATIENT,
    // PDE,
    PRESCRIPTION,
    // SNF,
  }
  
  /**
   * Day-Month-Year date format.
   */
  private static final SimpleDateFormat BB2_DATE_FORMAT = new SimpleDateFormat("dd-MMM-yyyy");

  /**
   * Get a date string in the format DD-MMM-YY from the given time stamp.
   */
  private static String bb2DateFromTimestamp(long time) {
    synchronized (BB2_DATE_FORMAT) {
      // http://bugs.java.com/bugdatabase/view_bug.do?bug_id=6231579
      return BB2_DATE_FORMAT.format(new Date(time));
    }
  }
  
  private boolean testing = true;  // specifies if we are testing, which causes some configs to be consistent from run to run (e.g., distributions)

  private File configFile;
  private Random rand = new Random();

  private List<BFDExportConfigEntry> allConfigs = null;
  
  private List<BFDExportConfigEntry> beneficiaryConfigs = new ArrayList<BFDExportConfigEntry>();
  private List<BFDExportConfigEntry> beneficiaryHistoryConfigs = new ArrayList<BFDExportConfigEntry>();
  private List<BFDExportConfigEntry> carrierConfigs = new ArrayList<BFDExportConfigEntry>();
  // private List<BFDExportConfigEntry> dmeConfigs = new ArrayList<BFDExportConfigEntry>();
  private List<BFDExportConfigEntry> inpatientConfigs = new ArrayList<BFDExportConfigEntry>();
  // private List<BFDExportConfigEntry> hhaConfigs = new ArrayList<BFDExportConfigEntry>();
  // private List<BFDExportConfigEntry> hospiceConfigs = new ArrayList<BFDExportConfigEntry>();
  // private List<BFDExportConfigEntry> medicareBeneficiaryIdConfigs = new ArrayList<BFDExportConfigEntry>();
  private List<BFDExportConfigEntry> outpatientConfigs = new ArrayList<BFDExportConfigEntry>();
  // private List<BFDExportConfigEntry> pdeConfigs = new ArrayList<BFDExportConfigEntry>();
  private List<BFDExportConfigEntry> prescriptionConfigs = new ArrayList<BFDExportConfigEntry>();
  // private List<BFDExportConfigEntry> snfConfigs = new ArrayList<BFDExportConfigEntry>();
  
  /** constructor */
  public BFDExportBuilder() {
    this.configFile = new File( "src/main/resources/exporters/cms_field_values2.tsv" );
    this.initConfigs();
  }

  /** determines if an expression in the TSV file should be added or ignored
   *  @param expression the expression to evaluate
   *  @return true iff this expression is can be evaluated, expanded and is useful
   */
  private boolean shouldAdd( String expression, BFDExportConfigEntry entry, ExportConfigType type ) {
    boolean retval = false;
    expression = expression.trim();
    if ( !expression.isEmpty() ) {
      // values we can work with
      if ( expression.equalsIgnoreCase("NULL") || expression.equalsIgnoreCase("Coded") ) {
        retval = false;
      }
      // things that still needs to be taken care of, write warning
      else if ( expression.startsWith("(") // comment only comment
                // || expression.startsWith("[:") // either [:...] or function call
      ) {
        System.out.printf("  config spreadsheet needs further work (line %3d | %-19s : %s : %s\n",
          entry.getLineNumber(), type, entry.getField(), expression);
        retval = false;
      }
      else if ( expression.startsWith("[")) { // potentially a function or developer note
        // switch ( expression ) {
        //   case "[Blank]":
        //   case "[bb2DateFromEncounterStartTimestamp]":
        //     retval = true;
        //     break;
        //   default:  // does not know called function, need to implement
        //     System.out.printf("  config spreadsheet needs further work (line %3d | %-19s)  %s:%s\n",
        //       entry.getLineNumber(), type, entry.getField(), expression);
        //     retval = false;
        //     break;
        // }
        retval = true;
      }
      else {
        retval = true;
      }
    }
    else {
      // System.out.println("rejecting because expression="+expression);
      retval = false;
    }
    return retval;
  }

  private String evalConfig( String expression, BFDExportConfigEntry entry, ExportConfigType type, HealthRecord.Encounter encounter ) {
    // look for comments (matching anything in parenthesis)
    //  currently we just ignore comments since they are for the analyst doing the config spreadsheet
    // String comment = null;
    String retval = expression;
    int commentStart = expression.indexOf("(");
    if ( commentStart >= 0 ) {
      retval = expression.substring(0, commentStart - 1);
      // comment = expression.substring(commentStart + 1, expression.length()-1);
    }
    boolean printError = false;

    // evaluate for functions
    if ( expression.startsWith("[")) {
      switch ( expression ) {
        case "[Blank]":
          // System.out.println("blank inserted");
          retval = "";
          break;
        case "[bb2Date_EncounterStartTimestamp]":
          retval = bb2DateFromTimestamp(encounter.start);
          break;
        case "[bb2Date_EncounterStopTimestamp]":
          retval = bb2DateFromTimestamp(encounter.stop);
          break;
        default:  // does not know called function, need to implement
          printError = true;
          retval = "";
          break;
      }
    }
    else if ( expression.startsWith("fieldValues.put") ) {  // same as [:...]
      printError = true;
      retval = "";
    }

    if ( printError ) {
      System.out.printf("  output configuration error: exporter does not know how to evaluate function on line %3d for %-19s:  %s:%s\n",
        entry.getLineNumber(), type, entry.getField(), expression);
    }
    return retval;
  }

  /** choose a value in a distribution string; note that this is safe to always use 
   *  since it will check to see if the the expression is a distribution string first
   *  @param expression the string that potentially represents a distribution string
   *  @param useFirst boolean to always use the first value in the distribution string;
   *                  useful for testing
  */
  private String evalConfigDistribution( String expression, boolean useFirst ) {
    // must be done after expression has removed things like comments and functions
    String retval = expression;
    if ( expression.contains(",") ) {
      List<String> values = Arrays.asList(retval.split(","));
      if ( useFirst ) {
        retval = values.get(0);
      }
      else {
        // System.out.println("flat distribution:"+expression);
        int index = this.rand.nextInt(values.size());
        retval = values.get(index);
      }
    }
    return retval;
  }

  /** initialize object from configuration file */
  private List<BFDExportConfigEntry> initConfigs() {
    try {
      System.out.println("Reading from " + this.configFile.getAbsolutePath() );
      Reader reader = new BufferedReader(new FileReader(this.configFile));
      CsvToBean<BFDExportConfigEntry> csvReader = new CsvToBeanBuilder<BFDExportConfigEntry>(reader)
            .withType(BFDExportConfigEntry.class)
            .withSeparator('\t')
            .withIgnoreLeadingWhiteSpace(true)
            .withIgnoreEmptyLine(true)
            .build();
      this.allConfigs = csvReader.parse();
      // for ( int i=0; i < 5; i++ ) {
      //   System.out.println(" allConfigs." + i + ": " + this.allConfigs.get(i));
      // }

      // this.initConfigItems();
      for ( BFDExportConfigEntry prop: this.getAllConfigs() ) {
        if ( shouldAdd(prop.getBeneficiary(), prop, ExportConfigType.BENEFICIARY) ) {
          this.beneficiaryConfigs.add(prop);
        }
        if ( shouldAdd(prop.getBeneficiary_history(), prop, ExportConfigType.BENEFICIARY_HISTORY) ) {
          this.beneficiaryHistoryConfigs.add(prop);
        }
        if ( shouldAdd(prop.getCarrier(), prop, ExportConfigType.CARRIER) ) {
          this.carrierConfigs.add(prop);
        }
        if ( shouldAdd(prop.getInpatient(), prop, ExportConfigType.INPATIENT) ) {
          this.inpatientConfigs.add(prop);
        }
        if ( shouldAdd(prop.getOutpatient(), prop, ExportConfigType.OUTPATIENT) ) {
          this.outpatientConfigs.add(prop);
        }
        if ( shouldAdd(prop.getPrescription(), prop, ExportConfigType.PRESCRIPTION) ) {
          this.prescriptionConfigs.add(prop);
        }
      } 
      return this.allConfigs;
    }
    catch ( IOException ex ) {
      System.out.println( "Error reading " + this.configFile.getAbsolutePath() );
      return null;
    }
  }

  /** Sets the known field values based on exporter config TSV file.
   * @param type output type (one of the ExportConfigType types)
   * @param fieldValues reference to a HashMap of field values in each of the exportXXXXX() functions
   * @param getCellValueFunc reference to Function that retrieves the string expression relevant to the current output type from the config file
   * @param getFieldEnumFunc reference to Function that retrieves the enum relevant to the current output type
   * @return the updated field values
   */
  public HashMap setFromConfig(ExportConfigType type, 
                                HashMap fieldValues, 
                                Function<BFDExportConfigEntry, String> getCellValueFunc, 
                                Function<String, Enum> getFieldEnumFunc,
                                HealthRecord.Encounter encounter) {
    fieldValues.clear();
    List<BFDExportConfigEntry> configs = this.getConfigItemsByType(type);
    try {
      int propCount = 0;
      for ( BFDExportConfigEntry prop: configs ) {
        String cell = getCellValueFunc.apply( prop );
        // System.out.println("*****"+cell);
        if ( !cell.isEmpty() ) {
          propCount++;
          String value = evalConfig(cell, prop, type, encounter);
          value = evalConfigDistribution( value, this.testing );
          Enum fieldEnum = getFieldEnumFunc.apply(prop.getField());
          fieldValues.put(fieldEnum, value);
        }
      }
      System.out.println("config props defined and processed for " + type + ":  " + propCount );
    }
    catch (Exception ex) {
      System.out.println("ExportDataBuilder.setFromConfig ERROR:  " + ex);
    }
    return fieldValues;
  }

  /** returns all the config items. */
  public List<BFDExportConfigEntry> getAllConfigs() {
    return this.allConfigs;
  }


  /** returns all inpatient config items. */
  public List<BFDExportConfigEntry> getConfigItemsByType(ExportConfigType type) {
    switch( type ) {
      case BENEFICIARY:
        return this.beneficiaryConfigs;
      case BENEFICIARY_HISTORY:
        return this.beneficiaryHistoryConfigs;
      case CARRIER:
        return this.carrierConfigs;
      case INPATIENT: 
        return this.inpatientConfigs;
      case OUTPATIENT: 
        return this.outpatientConfigs;
      case PRESCRIPTION: 
        return this.prescriptionConfigs;
      default: return null;
    }
  }
}

