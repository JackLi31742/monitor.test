package org.cripac.isee.vpe.util.logging;/*
 * This file is part of LaS-VPE Platform.
 *
 * LaS-VPE Platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * LaS-VPE Platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with LaS-VPE Platform.  If not, see <http://www.gnu.org/licenses/>.
 */

import org.apache.log4j.Level;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * A simple logger for output logs to the console only.
 * <p>
 * Created by ken.yu on 16-10-24.
 */
public class ConsoleLogger extends Logger implements Serializable {

    public ConsoleLogger() {
        super(Level.INFO);
    }

    public ConsoleLogger(Level level) {
        super(level);
    }

    private final static SimpleDateFormat ft = new SimpleDateFormat("yy.MM.dd HH:mm:ss");

    private String wrapMsg(Object msg) {
        return ft.format(new Date()) + "\t" + localName + "\t" + msg;
    }

    @Override
    public void debug(@Nonnull Object msg) {
        if (Level.DEBUG.isGreaterOrEqual(level)) {
            System.out.println("[DEBUG]\t" + wrapMsg(msg));
        }
    }

    @Override
    public void debug(@Nonnull Object msg,
                      @Nonnull Throwable t) {
        if (Level.DEBUG.isGreaterOrEqual(level)) {
            System.out.println("[DEBUG]\t" + wrapMsg(msg));
            t.printStackTrace();
        }
    }

    @Override
    public void info(@Nonnull Object msg) {
        if (Level.INFO.isGreaterOrEqual(level)) {
            System.out.println("[INFO]\t" + wrapMsg(msg));
        }
    }

    @Override
    public void info(@Nonnull Object msg,
                     @Nonnull Throwable t) {
        if (Level.INFO.isGreaterOrEqual(level)) {
            System.out.println("[INFO]\t" + wrapMsg(msg));
            t.printStackTrace();
        }
    }

    @Override
    public void warn(@Nonnull Object msg) {
        if (Level.WARN.isGreaterOrEqual(level)) {
            System.out.println("[WARNING]\t" + wrapMsg(msg));
        }
    }

    @Override
    public void warn(@Nonnull Object msg,
                     @Nonnull Throwable t) {
        if (Level.WARN.isGreaterOrEqual(level)) {
            System.out.println("[WARNING]\t" + wrapMsg(msg));
            t.printStackTrace();
        }
    }

    @Override
    public void error(@Nonnull Object msg) {
        if (Level.ERROR.isGreaterOrEqual(level)) {
            System.err.println("[ERROR]\t" + wrapMsg(msg));
        }
    }

    @Override
    public void error(@Nonnull Object msg,
                      @Nonnull Throwable t) {
        if (Level.ERROR.isGreaterOrEqual(level)) {
            System.err.println("[ERROR]\t" + wrapMsg(msg));
            t.printStackTrace();
        }
    }

    @Override
    public void fatal(@Nonnull Object msg) {
        if (Level.FATAL.isGreaterOrEqual(level)) {
            System.err.println("[FATAL]\t" + wrapMsg(msg));
        }
    }

    @Override
    public void fatal(@Nonnull Object msg,
                      @Nonnull Throwable t) {
        if (Level.FATAL.isGreaterOrEqual(level)) {
            System.err.println("[FATAL]\t" + wrapMsg(msg));
            t.printStackTrace();
        }
    }
}
