/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxutils.stats;

import java.io.PrintStream;
import java.util.Collection;

/**
 * Helper class to print statistics.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public final class PrintStatistics {

    /**
     * Static class
     */
    private PrintStatistics() {

    }

    /**
     * Print the statistics to a stream.
     *
     * @param p_outputStream
     *         Output stream to print to.
     */
    public static void printStatisticsToOutput(final PrintStream p_outputStream) {
        p_outputStream.println("---------------------------------------------------------");
        p_outputStream.println("---------------------------------------------------------");
        p_outputStream.println("---------------------------------------------------------");

        Collection<StatisticsRecorder> recorders = Statistics.getRecorders();
        for (StatisticsRecorder recorder : recorders) {
            p_outputStream.println(recorder);
            p_outputStream.println("---------------------------------------------------------");
        }

        p_outputStream.println("---------------------------------------------------------");
        p_outputStream.println("---------------------------------------------------------");
    }
}
