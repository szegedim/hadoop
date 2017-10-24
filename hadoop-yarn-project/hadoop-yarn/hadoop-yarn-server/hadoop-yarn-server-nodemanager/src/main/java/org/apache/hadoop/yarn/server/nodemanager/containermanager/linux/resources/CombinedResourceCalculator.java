package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ProcfsBasedProcessTree;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;

public class CombinedResourceCalculator  extends ResourceCalculatorProcessTree {
  protected static final Log LOG = LogFactory
      .getLog(CombinedResourceCalculator.class);
  private ProcfsBasedProcessTree legacy;
  private CGroupsResourceCalculator modern;

  public CombinedResourceCalculator(String pid) {
    super(pid);
    legacy = new ProcfsBasedProcessTree(pid);
    try {
      modern = new CGroupsResourceCalculator(pid);
      modern.setCGroupFilePaths();
    } catch (YarnException ex) {
      LOG.error(ex.getMessage(), ex);
    }
  }

  @Override
  public void updateProcessTree() {

  }

  @Override
  public String getProcessTreeDump() {
    return null;
  }

  @Override
  public float getCpuUsagePercent() {
    legacy.updateProcessTree();
    float value1 = legacy.getCpuUsagePercent();
    float value2 = modern.getCpuUsagePercent();
    LOG.info("CPU Comparison:" + value1 + " " + value2);
    LOG.info("Jiffy Comparison:" +
        legacy.getCumulativeCpuTime() + " " + modern.getCumulativeCpuTime());

    return value2;
  }

  @Override
  public boolean checkPidPgrpidForMatch() {
    return false;
  }

  @Override
  public long getCumulativeCpuTime() {
    return modern.getCumulativeCpuTime();
  }

  @Override
  public long getRssMemorySize(int olderThanAge) {
    legacy.updateProcessTree();
    LOG.info("MEM Comparison:" +
        legacy.getRssMemorySize(olderThanAge) + " " +
        modern.getRssMemorySize(olderThanAge));
    return legacy.getRssMemorySize(olderThanAge);
  }

  @Override
  public long getVirtualMemorySize(int olderThanAge) {
    legacy.updateProcessTree();
    LOG.info("VMEM Comparison:" +
        legacy.getVirtualMemorySize(olderThanAge) + " " +
        modern.getVirtualMemorySize(olderThanAge));
    return legacy.getVirtualMemorySize(olderThanAge);
  }
}
