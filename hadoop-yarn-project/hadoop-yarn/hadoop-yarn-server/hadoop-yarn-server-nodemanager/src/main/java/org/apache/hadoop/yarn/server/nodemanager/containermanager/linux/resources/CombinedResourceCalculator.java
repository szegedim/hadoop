package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ProcfsBasedProcessTree;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;

/**
 * CombinedResourceCalculator is a resource calculator that uses cgroups but
 * it is backward compatible with procfs in terms of virtual memory usage.
 */
public class CombinedResourceCalculator  extends ResourceCalculatorProcessTree {
  protected static final Log LOG = LogFactory
      .getLog(CombinedResourceCalculator.class);
  private ProcfsBasedProcessTree procfs;
  private CGroupsResourceCalculator cgroup;

  public CombinedResourceCalculator(String pid) {
    super(pid);
    procfs = new ProcfsBasedProcessTree(pid);
    cgroup = new CGroupsResourceCalculator(pid);
  }

  public void setCGroupFilePaths() throws YarnException {
    cgroup.setCGroupFilePaths();
  }

  @Override
  public void updateProcessTree() {
    procfs.updateProcessTree();
    cgroup.updateProcessTree();
  }

  @Override
  public String getProcessTreeDump() {
    return null;
  }

  @Override
  public float getCpuUsagePercent() {
    float cgroupUsage = cgroup.getCpuUsagePercent();
    if (LOG.isDebugEnabled()) {
      float procfsUsage = procfs.getCpuUsagePercent();
      LOG.debug("CPU Comparison:" + procfsUsage + " " + cgroupUsage);
      LOG.debug("Jiffy Comparison:" +
          procfs.getCumulativeCpuTime() + " " +
          cgroup.getCumulativeCpuTime());
    }

    return cgroupUsage;
  }

  @Override
  public boolean checkPidPgrpidForMatch() {
    return procfs.checkPidPgrpidForMatch();
  }

  @Override
  public long getCumulativeCpuTime() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("CPU Comparison:" +
          procfs.getCumulativeCpuTime() + " " +
          cgroup.getCumulativeCpuTime());
    }
    return cgroup.getCumulativeCpuTime();
  }

  @Override
  public long getRssMemorySize(int olderThanAge) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("MEM Comparison:" +
          procfs.getRssMemorySize(olderThanAge) + " " +
          cgroup.getRssMemorySize(olderThanAge));
    }
    return cgroup.getRssMemorySize(olderThanAge);
  }

  @Override
  public long getVirtualMemorySize(int olderThanAge) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("VMEM Comparison:" +
          procfs.getVirtualMemorySize(olderThanAge) + " " +
          cgroup.getVirtualMemorySize(olderThanAge));
    }
    return procfs.getVirtualMemorySize(olderThanAge);
  }
}
