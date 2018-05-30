/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.job;

import java.util.concurrent.atomic.AtomicLong;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.job.ws.Worker;
import de.hhu.bsinfo.dxram.job.ws.WorkerDelegate;

/**
 * Implementation of a JobComponent using a work stealing approach for scheduling/load balancing.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class JobWorkStealingComponent extends AbstractJobComponent<JobWorkStealingComponentConfig> implements WorkerDelegate {
    // component dependencies
    private AbstractBootComponent m_boot;

    private Worker[] m_workers;
    private AtomicLong m_unfinishedJobs = new AtomicLong(0);

    /**
     * Constructor
     */
    public JobWorkStealingComponent() {
        super(DXRAMComponentOrder.Init.JOB_WORK_STEALING, DXRAMComponentOrder.Shutdown.JOB_WORK_STEALING, JobWorkStealingComponentConfig.class);
    }

    @Override
    public boolean pushJob(final AbstractJob p_job) {
        // cause we are using a work stealing approach, we do not need to
        // care about which worker to assign this job to

        boolean success = false;
        for (Worker worker : m_workers) {
            if (worker.pushJob(p_job)) {
                // causes the garbage collector to go crazy if too many jobs are pushed very quickly
                // #if LOGGER >= DEBUG
                LOGGER.debug("Submitted job %s to worker %s", p_job, worker);
                // #endif /* LOGGER >= DEBUG */

                success = true;
                break;
            }
        }

        // #if LOGGER >= WARN
        if (!success) {
            LOGGER.warn("Submiting job %s failed", p_job);
        }
        // #endif /* LOGGER >= WARN */

        return success;
    }

    @Override
    public long getNumberOfUnfinishedJobs() {
        return m_unfinishedJobs.get();
    }

    @Override
    public boolean waitForSubmittedJobsToFinish() {
        while (m_unfinishedJobs.get() > 0) {
            Thread.yield();
        }

        return true;
    }

    @Override
    protected boolean supportsSuperpeer() {
        return false;
    }

    @Override
    protected boolean supportsPeer() {
        return true;
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
    }

    @Override
    protected boolean initComponent(final DXRAMContext.Config p_config) {
        m_workers = new Worker[getConfig().getNumWorkers()];

        for (int i = 0; i < m_workers.length; i++) {
            m_workers[i] = new Worker(i, this);
        }

        // avoid race condition by first creating all workers, then starting them
        for (Worker worker : m_workers) {
            worker.start();
        }

        // wait until all workers are running
        for (Worker worker : m_workers) {
            while (!worker.isRunning()) {
            }
        }

        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        // #if LOGGER >= DEBUG
        LOGGER.debug("Waiting for unfinished jobs...");
        // #endif /* LOGGER >= DEBUG */

        while (m_unfinishedJobs.get() > 0) {
            Thread.yield();
        }

        for (Worker worker : m_workers) {
            worker.shutdown();
        }

        // #if LOGGER >= DEBUG
        LOGGER.debug("Waiting for workers to shut down...");
        // #endif /* LOGGER >= DEBUG */

        for (Worker worker : m_workers) {
            while (worker.isRunning()) {
                Thread.yield();
            }
        }

        return true;
    }

    @Override
    public AbstractJob stealJobLocal(final Worker p_thief) {
        AbstractJob job = null;

        for (Worker worker : m_workers) {
            // don't steal from own queue
            if (p_thief == worker) {
                continue;
            }

            job = worker.stealJob();
            if (job != null) {
                // #if LOGGER == TRACE
                LOGGER.trace("Job %s stolen from worker %s", job, worker);
                // #endif /* LOGGER == TRACE */
                break;
            }
        }

        return job;
    }

    @Override
    public void scheduledJob(final AbstractJob p_job) {
        m_unfinishedJobs.incrementAndGet();
        p_job.notifyListenersJobScheduledForExecution(m_boot.getNodeID());
    }

    @Override
    public void executingJob(final AbstractJob p_job) {
        p_job.notifyListenersJobStartsExecution(m_boot.getNodeID());
    }

    @Override
    public void finishedJob(final AbstractJob p_job) {
        m_unfinishedJobs.decrementAndGet();
        p_job.notifyListenersJobFinishedExecution(m_boot.getNodeID());
    }

    @Override
    public short getNodeID() {
        return m_boot.getNodeID();
    }
}
