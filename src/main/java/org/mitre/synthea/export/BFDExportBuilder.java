package org.mitre.synthea.export;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
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

  private boolean shouldAdd( String value ) {
    boolean retval = false;
    value = value.trim();
    if ( !value.isEmpty() ) {
      // values we can work with
      if ( value.equalsIgnoreCase("NULL") || value.startsWith("Coded") ) {
        retval = false;
      }
      // things that still needs to be taken care of, write warning
      else if ( value.startsWith("Mapped from ")
                || value.startsWith("fieldValues.put")
                || value.startsWith("(")
                || value.startsWith("logic exists ")
                || value.startsWith("RxNorm to")
                || value.startsWith("if (")
                || value.startsWith("bb2DateFrom")
      ) {
        System.out.println("Config spreadsheet needs further work:"+value);
        retval = false;
      }
      else {
        retval = true;
      }
    }
    else {
      // System.out.println("rejecting because value="+value);
      retval = false;
    }
    return retval;
  }

  private String evalConfig( String value ) {
    // look for comments (matching anything in parenthesis)
    //  currently we just ignore comments since they are for the analyst doing the config spreadsheet
    // String comment = null;
    String retval = value;
    int commentStart = value.indexOf("(");
    if ( commentStart >= 0 ) {
      retval = value.substring(0, commentStart - 1);
      // comment = value.substring(commentStart + 1, value.length()-1);
    }

    // replace [Blank] with empty string
    if ( value.equalsIgnoreCase("[Blank]")) {
      // System.out.println("blank inserted");
      retval = "";
    }

    // distributions and functions are done at export phase
    return retval;
  }

  /** choose a value in a distribution string; note that this is safe to always use 
   *  since it will check to see if the the expression is a distribution string first
   *  @param expression the string that potentially represents a distribution string
   *  @param useFirst boolean to always use the first value in the distribution string;
   *                  useful for testing
  */
  private String evalConfigDistribution( String expression, boolean useFirst ) {
    // must be done after value has removed things like comments and functions
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
      System.out.println("reading from " + this.configFile.getAbsolutePath() );
      Reader reader = new BufferedReader(new FileReader(this.configFile));
      CsvToBean<BFDExportConfigEntry> csvReader = new CsvToBeanBuilder<BFDExportConfigEntry>(reader)
            .withType(BFDExportConfigEntry.class)
            .withSeparator('\t')
            .withIgnoreLeadingWhiteSpace(true)
            .withIgnoreEmptyLine(true)
            .build();
      this.allConfigs = csvReader.parse();
      for ( int i=0; i < 5; i++ ) {
        System.out.println(" allConfigs." + i + ": " + this.allConfigs.get(i));
      }

      // this.initConfigItems();
      for ( BFDExportConfigEntry prop: this.getAllConfigs() ) {
        if ( shouldAdd(prop.getBeneficiary()) ) {
          this.beneficiaryConfigs.add(prop);
        }
        if ( shouldAdd(prop.getBeneficiary_history()) ) {
          this.beneficiaryHistoryConfigs.add(prop);
        }
        if ( shouldAdd(prop.getCarrier()) ) {
          this.carrierConfigs.add(prop);
        }
        if ( shouldAdd(prop.getInpatient()) ) {
          this.inpatientConfigs.add(prop);
        }
        if ( shouldAdd(prop.getOutpatient()) ) {
          this.outpatientConfigs.add(prop);
        }
        if ( shouldAdd(prop.getPrescription()) ) {
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
   * @param getCellValueFunc reference to Function that retrieves the string value relevant to the current output type from the config file
   * @param getFieldEnumFunc reference to Function that retrieves the enum relevant to the current output type
   * @return the updated field values
   */
  public HashMap setFromConfig(ExportConfigType type, 
                                HashMap fieldValues, 
                                Function<BFDExportConfigEntry, String> getCellValueFunc, 
                                Function<String, Enum> getFieldEnumFunc) {
    fieldValues.clear();
    List<BFDExportConfigEntry> configs = this.getConfigItemsByType(type);
    try {
      int propCount = 0;
      for ( BFDExportConfigEntry prop: configs ) {
        String cell = getCellValueFunc.apply( prop );
        // System.out.println("*****"+cell);
        if ( !cell.isEmpty() ) {
          propCount++;
          String value = evalConfig(cell);
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

