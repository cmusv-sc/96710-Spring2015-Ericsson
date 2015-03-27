/*
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/*
 *    CfsSubsetEval.java
 *    Copyright (C) 1999-2012 University of Waikato, Hamilton, New Zealand
 *
 */

package weka.attributeSelection;

import java.util.BitSet;
import java.util.Enumeration;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import weka.classifiers.Classifier;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.evaluation.Evaluation;
import weka.core.Capabilities;
import weka.core.Capabilities.Capability;
import weka.core.Debug;
import weka.core.Instances;
import weka.core.Option;
import weka.core.OptionHandler;
import weka.core.RevisionUtils;
import weka.core.TechnicalInformation;
import weka.core.TechnicalInformation.Field;
import weka.core.TechnicalInformation.Type;
import weka.core.TechnicalInformationHandler;
import weka.core.ThreadSafe;
import weka.core.Utils;
import weka.filters.Filter;
import weka.filters.supervised.attribute.Discretize;
import weka.filters.unsupervised.attribute.Remove;

/**
 * <!-- globalinfo-start --> CfsSubsetEval :<br/>
 * <br/>
 * Evaluates the worth of a subset of attributes by considering the individual
 * predictive ability of each feature along with the degree of redundancy
 * between them.<br/>
 * <br/>
 * Subsets of features that are highly correlated with the class while having
 * low intercorrelation are preferred.<br/>
 * <br/>
 * For more information see:<br/>
 * <br/>
 * M. A. Hall (1998). Correlation-based Feature Subset Selection for Machine
 * Learning. Hamilton, New Zealand.
 * <p/>
 * <!-- globalinfo-end -->
 * 
 * <!-- technical-bibtex-start --> BibTeX:
 * 
 * <pre>
 * &#64;phdthesis{Hall1998,
 *    address = {Hamilton, New Zealand},
 *    author = {M. A. Hall},
 *    school = {University of Waikato},
 *    title = {Correlation-based Feature Subset Selection for Machine Learning},
 *    year = {1998}
 * }
 * </pre>
 * <p/>
 * <!-- technical-bibtex-end -->
 * 
 * <!-- options-start --> Valid options are:
 * <p/>
 * 
 * <pre>
 * -M
 *  Treat missing values as a separate value.
 * </pre>
 * 
 * <pre>
 * -L
 *  Don't include locally predictive attributes.
 * </pre>
 * 
 * <pre>
 * -Z
 *  Precompute the full correlation matrix at the outset, rather than compute correlations lazily (as needed) during the search. Use this in conjuction with parallel processing in order to speed up a backward search.
 * </pre>
 * 
 * <pre>
 * -P &lt;int&gt;
 *  The size of the thread pool, for example, the number of cores in the CPU. (default 1)
 * </pre>
 * 
 * <pre>
 * -E &lt;int&gt;
 *  The number of threads to use, which should be &gt;= size of thread pool. (default 1)
 * </pre>
 * 
 * <pre>
 * -D
 *  Output debugging info.
 * </pre>
 * 
 * <!-- options-end -->
 * 
 * @author Mark Hall (mhall@cs.waikato.ac.nz)
 * @version $Revision: 11215 $
 * @see Discretize
 */
public class CrossRefBayesianValidation extends ASEvaluation implements SubsetEvaluator,
  ThreadSafe, OptionHandler, TechnicalInformationHandler {

  /** for serialization */
  static final long serialVersionUID = 747878400813276317L;

  /** The training instances */
  private Instances m_trainInstances;
  /** The testing instances */
  private Evaluation m_Evaluation;
  /** The class index */
  private int m_classIndex;
  /** Number of attributes in the training data */
  private int m_numAttribs;
  /** Treat missing values as separate values */
  private boolean m_missingSeparate;
  /** Include locally predictive attributes */
  private boolean m_locallyPredictive;
  /** classifier instance */
  private Classifier m_Classifier;
  
  private Debug.SimpleLog m_debuglog;

  /** Output debugging info */
  protected boolean m_debug;

  /** Number of entries in the correlation matrix */
  protected int m_numEntries;

  /** Number of correlations actually computed */
  protected AtomicInteger m_numFilled;

  protected boolean m_preComputeCorrelationMatrix;

  /**
   * The number of threads used to compute the correlation matrix. Used when
   * correlation matrix is precomputed
   */
  protected int m_numThreads = 1;

  /**
   * The size of the thread pool. Usually set equal to the number of CPUs or CPU
   * cores available
   */
  protected int m_poolSize = 1;

  /** Thread pool */
  protected transient ExecutorService m_pool = null;

  /**
   * Returns a string describing this attribute evaluator
   * 
   * @return a description of the evaluator suitable for displaying in the
   *         explorer/experimenter gui
   */
  public String globalInfo() {
    return "CfsSubsetEval :\n\nEvaluates the worth of a subset of attributes "
      + "by considering the individual predictive ability of each feature "
      + "along with the degree of redundancy between them.\n\n"
      + "Subsets of features that are highly correlated with the class "
      + "while having low intercorrelation are preferred.\n\n"
      + "For more information see:\n\n" + getTechnicalInformation().toString();
  }

  /**
   * Returns an instance of a TechnicalInformation object, containing detailed
   * information about the technical background of this class, e.g., paper
   * reference or book this class is based on.
   * 
   * @return the technical information about this class
   */
  @Override
  public TechnicalInformation getTechnicalInformation() {
    TechnicalInformation result;

    result = new TechnicalInformation(Type.PHDTHESIS);
    result.setValue(Field.AUTHOR, "M. A. Hall");
    result.setValue(Field.YEAR, "1998");
    result.setValue(Field.TITLE,
      "Correlation-based Feature Subset Selection for Machine Learning");
    result.setValue(Field.SCHOOL, "University of Waikato");
    result.setValue(Field.ADDRESS, "Hamilton, New Zealand");

    return result;
  }

  /**
   * Constructor
   */
  public CrossRefBayesianValidation() {
    resetOptions();
  }

  /**
   * Returns an enumeration describing the available options.
   * 
   * @return an enumeration of all the available options.
   * 
   **/
  @Override
  public Enumeration<Option> listOptions() {
    Vector<Option> newVector = new Vector<Option>(6);
    newVector.addElement(new Option("\tTreat missing values as a separate "
      + "value.", "M", 0, "-M"));
    newVector.addElement(new Option(
      "\tDon't include locally predictive attributes" + ".", "L", 0, "-L"));

    newVector.addElement(new Option(
      "\t" + preComputeCorrelationMatrixTipText(), "Z", 0, "-Z"));

    newVector.addElement(new Option(
      "\t" + poolSizeTipText() + " (default 1)\n", "P", 1, "-P <int>"));
    newVector.addElement(new Option("\t" + numThreadsTipText()
      + " (default 1)\n", "E", 1, "-E <int>"));
    newVector.addElement(new Option("\tOutput debugging info" + ".", "D", 0,
      "-D"));
    return newVector.elements();
  }

  /**
   * Parses and sets a given list of options.
   * <p/>
   * 
   * <!-- options-start --> Valid options are:
   * <p/>
   * 
   * <pre>
   * -M
   *  Treat missing values as a separate value.
   * </pre>
   * 
   * <pre>
   * -L
   *  Don't include locally predictive attributes.
   * </pre>
   * 
   * <pre>
   * -Z
   *  Precompute the full correlation matrix at the outset, rather than compute correlations lazily (as needed) during the search. Use this in conjuction with parallel processing in order to speed up a backward search.
   * </pre>
   * 
   * <pre>
   * -P &lt;int&gt;
   *  The size of the thread pool, for example, the number of cores in the CPU. (default 1)
   * </pre>
   * 
   * <pre>
   * -E &lt;int&gt;
   *  The number of threads to use, which should be &gt;= size of thread pool. (default 1)
   * </pre>
   * 
   * <pre>
   * -D
   *  Output debugging info.
   * </pre>
   * 
   * <!-- options-end -->
   * 
   * @param options the list of options as an array of strings
   * @throws Exception if an option is not supported
   * 
   **/
  @Override
  public void setOptions(String[] options) throws Exception {

    resetOptions();
    setMissingSeparate(Utils.getFlag('M', options));
    setLocallyPredictive(!Utils.getFlag('L', options));
    setPreComputeCorrelationMatrix(Utils.getFlag('Z', options));

    String PoolSize = Utils.getOption('P', options);
    if (PoolSize.length() != 0) {
      setPoolSize(Integer.parseInt(PoolSize));
    } else {
      setPoolSize(1);
    }
    String NumThreads = Utils.getOption('E', options);
    if (NumThreads.length() != 0) {
      setNumThreads(Integer.parseInt(NumThreads));
    } else {
      setNumThreads(1);
    }

    setDebug(Utils.getFlag('D', options));
  }

  /**
   * @return a string to describe the option
   */
  public String preComputeCorrelationMatrixTipText() {
    return "Precompute the full correlation matrix at the outset, "
      + "rather than compute correlations lazily (as needed) "
      + "during the search. Use this in conjuction with "
      + "parallel processing in order to speed up a backward " + "search.";
  }

  /**
   * Set whether to pre-compute the full correlation matrix at the outset,
   * rather than computing individual correlations lazily (as needed) during the
   * search.
   * 
   * @param p true if the correlation matrix is to be pre-computed at the outset
   */
  public void setPreComputeCorrelationMatrix(boolean p) {
    m_preComputeCorrelationMatrix = p;
  }

  /**
   * Get whether to pre-compute the full correlation matrix at the outset,
   * rather than computing individual correlations lazily (as needed) during the
   * search.
   * 
   * @return true if the correlation matrix is to be pre-computed at the outset
   */
  public boolean getPreComputeCorrelationMatrix() {
    return m_preComputeCorrelationMatrix;
  }

  /**
   * @return a string to describe the option
   */
  public String numThreadsTipText() {

    return "The number of threads to use, which should be >= size of thread pool.";
  }

  /**
   * Gets the number of threads.
   */
  public int getNumThreads() {

    return m_numThreads;
  }

  /**
   * Sets the number of threads
   */
  public void setNumThreads(int nT) {

    m_numThreads = nT;
  }

  /**
   * @return a string to describe the option
   */
  public String poolSizeTipText() {

    return "The size of the thread pool, for example, the number of cores in the CPU.";
  }

  /**
   * Gets the number of threads.
   */
  public int getPoolSize() {

    return m_poolSize;
  }

  /**
   * Sets the number of threads
   */
  public void setPoolSize(int nT) {

    m_poolSize = nT;
  }

  /**
   * Returns the tip text for this property
   * 
   * @return tip text for this property suitable for displaying in the
   *         explorer/experimenter gui
   */
  public String locallyPredictiveTipText() {
    return "Identify locally predictive attributes. Iteratively adds "
      + "attributes with the highest correlation with the class as long "
      + "as there is not already an attribute in the subset that has a "
      + "higher correlation with the attribute in question";
  }

  /**
   * Include locally predictive attributes
   * 
   * @param b true or false
   */
  public void setLocallyPredictive(boolean b) {
    m_locallyPredictive = b;
  }

  /**
   * Return true if including locally predictive attributes
   * 
   * @return true if locally predictive attributes are to be used
   */
  public boolean getLocallyPredictive() {
    return m_locallyPredictive;
  }

  /**
   * Returns the tip text for this property
   * 
   * @return tip text for this property suitable for displaying in the
   *         explorer/experimenter gui
   */
  public String missingSeparateTipText() {
    return "Treat missing as a separate value. Otherwise, counts for missing "
      + "values are distributed across other values in proportion to their "
      + "frequency.";
  }

  /**
   * Treat missing as a separate value
   * 
   * @param b true or false
   */
  public void setMissingSeparate(boolean b) {
    m_missingSeparate = b;
  }

  /**
   * Return true is missing is treated as a separate value
   * 
   * @return true if missing is to be treated as a separate value
   */
  public boolean getMissingSeparate() {
    return m_missingSeparate;
  }

  /**
   * Set whether to output debugging info
   * 
   * @param d true if debugging info is to be output
   */
  public void setDebug(boolean d) {
    m_debug = d;
  }

  /**
   * Set whether to output debugging info
   * 
   * @return true if debugging info is to be output
   */
  public boolean getDebug() {
    return m_debug;
  }

  /**
   * Returns the tip text for this property
   * 
   * @return tip text for this property suitable for displaying in the
   *         explorer/experimenter gui
   */
  public String debugTipText() {
    return "Output debugging info";
  }

  /**
   * Gets the current settings of CfsSubsetEval
   * 
   * @return an array of strings suitable for passing to setOptions()
   */
  @Override
  public String[] getOptions() {

    Vector<String> options = new Vector<String>();

    if (getMissingSeparate()) {
      options.add("-M");
    }

    if (!getLocallyPredictive()) {
      options.add("-L");
    }

    if (getPreComputeCorrelationMatrix()) {
      options.add("-Z");
    }

    options.add("-P");
    options.add("" + getPoolSize());

    options.add("-E");
    options.add("" + getNumThreads());

    if (getDebug()) {
      options.add("-D");
    }

    return options.toArray(new String[0]);
  }

  /**
   * Returns the capabilities of this evaluator.
   * 
   * @return the capabilities of this evaluator
   * @see Capabilities
   */
  @Override
  public Capabilities getCapabilities() {
    Capabilities result = super.getCapabilities();
    result.disableAll();

    // attributes
    result.enable(Capability.NOMINAL_ATTRIBUTES);
    result.enable(Capability.NUMERIC_ATTRIBUTES);
    result.enable(Capability.DATE_ATTRIBUTES);
    result.enable(Capability.MISSING_VALUES);

    // class
    result.enable(Capability.NOMINAL_CLASS);
    result.enable(Capability.NUMERIC_CLASS);
    result.enable(Capability.DATE_CLASS);
    result.enable(Capability.MISSING_CLASS_VALUES);

    return result;
  }

  /**
   * Generates a attribute evaluator. Has to initialize all fields of the
   * evaluator that are not being set via options.
   * 
   * CFS also discretises attributes (if necessary) and initializes the
   * correlation matrix.
   * 
   * @param data set of instances serving as training data
   * @throws Exception if the evaluator has not been generated successfully
   */
  @Override
  public void buildEvaluator(Instances data) throws Exception {

    // can evaluator handle data?
    getCapabilities().testWithFail(data);

    m_numEntries = 0;
    m_numFilled = new AtomicInteger();

    m_trainInstances = new Instances(data);
    m_trainInstances.deleteWithMissingClass();
    m_classIndex = m_trainInstances.classIndex();
    m_numAttribs = m_trainInstances.numAttributes();
    
	m_Classifier = new NaiveBayes();
	
	m_debuglog = new Debug.SimpleLog();

  }

  /**
   * evaluates a subset of attributes
   * 
   * @param subset a bitset representing the attribute subset to be evaluated
   * @return the merit
   * @throws Exception if the subset could not be evaluated
   */
  @Override
  public double evaluateSubset(BitSet subset) throws Exception {
	  
	  m_debuglog.log(subset.toString());
	  
	  int numAttributes = 0;
	  double errorRate = 0;
	  
	  Random rand = new Random(System.currentTimeMillis());
	  Instances trainInst = new Instances(m_trainInstances);
	  trainInst.randomize(rand);
	  
	  Remove delTransform = new Remove();
	  delTransform.setInvertSelection(true);
	  
	  for (int i = 0; i < m_numAttribs; i++)
	  {
		  if (subset.get(i) && i != m_classIndex)
		  {
			  numAttributes++;
		  }
	  }
	  
	  m_debuglog.log("Num attributes = " + numAttributes);
	  
	  if (numAttributes <= 0)
		  return Double.NEGATIVE_INFINITY;

	  // set up an array of attribute indexes for the filter
	  int[] featureArray = new int[numAttributes+1];
	  int i, j;
	  
	  for (i = 0, j = 0; i < m_numAttribs; i++) {
		  if (subset.get(i) && i != m_classIndex) {
			  featureArray[j++] = i;
		  }
	  }

	  featureArray[j] = m_classIndex;
	  
	  m_debuglog.log("Feature array = " + featureArray.toString());
	  
	  delTransform.setAttributeIndicesArray(featureArray);
	  delTransform.setInputFormat(trainInst);
	  trainInst = Filter.useFilter(trainInst, delTransform);

	  int folds = 3;
	  trainInst.stratify(folds);
	  
	  for (int n = 0; n < folds; n++)
	  {

		  m_debuglog.log("Eval fold = " + n);
		  
		  Instances train = trainInst.trainCV(folds, n);
		  Instances test =  trainInst.testCV(folds, n);
		   
		  // build the classifier
		  m_Classifier.buildClassifier(train);
		  
		  m_Evaluation = new Evaluation(train);
		  m_Evaluation.evaluateModel(m_Classifier, test);

		  if (m_trainInstances.classAttribute().isNominal()) {
			  errorRate += m_Evaluation.errorRate();
		  } else {
			  errorRate += m_Evaluation.meanAbsoluteError();
		  }

		  m_Evaluation = null;
		  // return the negative of the error as search methods need to
		  // maximize something
	  }
	  

	  return -errorRate;
	  
//    double num = 0.0;
//    double denom = 0.0;
//    float corr;
//    int larger, smaller;
//    // do numerator
//    for (int i = 0; i < m_numAttribs; i++) {
//      if (i != m_classIndex) {
//        if (subset.get(i)) {
//          if (i > m_classIndex) {
//            larger = i;
//            smaller = m_classIndex;
//          } else {
//            smaller = i;
//            larger = m_classIndex;
//          }
//          /*
//           * int larger = (i > m_classIndex ? i : m_classIndex); int smaller =
//           * (i > m_classIndex ? m_classIndex : i);
//           */
//          if (m_corr_matrix[larger][smaller] == -999) {
//            corr = correlate(i, m_classIndex);
//            m_corr_matrix[larger][smaller] = corr;
//            num += (m_std_devs[i] * corr);
//          } else {
//            num += (m_std_devs[i] * m_corr_matrix[larger][smaller]);
//          }
//        }
//      }
//    }
//
//    // do denominator
//    for (int i = 0; i < m_numAttribs; i++) {
//      if (i != m_classIndex) {
//        if (subset.get(i)) {
//          denom += (1.0 * m_std_devs[i] * m_std_devs[i]);
//
//          for (int j = 0; j < m_corr_matrix[i].length - 1; j++) {
//            if (subset.get(j)) {
//              if (m_corr_matrix[i][j] == -999) {
//                corr = correlate(i, j);
//                m_corr_matrix[i][j] = corr;
//                denom += (2.0 * m_std_devs[i] * m_std_devs[j] * corr);
//              } else {
//                denom +=
//                  (2.0 * m_std_devs[i] * m_std_devs[j] * m_corr_matrix[i][j]);
//              }
//            }
//          }
//        }
//      }
//    }
//
//    if (denom < 0.0) {
//      denom *= -1.0;
//    }
//
//    if (denom == 0.0) {
//      return (0.0);
//    }
//
//    double merit = (num / Math.sqrt(denom));
//
//    if (merit < 0.0) {
//      merit *= -1.0;
//    }
//
//    return merit;
  }

  /**
   * returns a string describing CFS
   * 
   * @return the description as a string
   */
  @Override
  public String toString() {
    StringBuffer text = new StringBuffer();

    if (m_trainInstances == null) {
      text.append("CFS subset evaluator has not been built yet\n");
    } else {
      text.append("\tCFS Subset Evaluator\n");

      if (m_missingSeparate) {
        text.append("\tTreating missing values as a separate value\n");
      }

      if (m_locallyPredictive) {
        text.append("\tIncluding locally predictive attributes\n");
      }
    }

    return text.toString();
  }

  protected void resetOptions() {
    m_trainInstances = null;
    m_missingSeparate = false;
    m_locallyPredictive = true;
  }

  /**
   * Returns the revision string.
   * 
   * @return the revision
   */
  @Override
  public String getRevision() {
    return RevisionUtils.extract("$Revision: 11215 $");
  }

  /**
   * Main method for testing this class.
   * 
   * @param args the options
   */
  public static void main(String[] args) {
    runEvaluator(new CrossRefBayesianValidation(), args);
  }
}
