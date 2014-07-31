/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.invocation;

import java.util.SortedMap;
import java.util.TreeMap;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.ExceptionFactory;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.balancer.Balancer;
import net.sf.hajdbc.dialect.Dialect;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;
import net.sf.hajdbc.sql.ProxyFactory;
import net.sf.hajdbc.state.StateManager;

/**
 * @author paul
 *
 */
public class InvokeOnContextInvocationStrategy<Z, D extends Database<Z>> implements InvocationStrategy
{
	private static Logger logger = LoggerFactory.getLogger(InvokeOnContextInvocationStrategy.class);

	private final D database;
	
	public InvokeOnContextInvocationStrategy(D database)
	{
		this.database = database;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public <ZZ, DD extends Database<ZZ>, T, R, E extends Exception> SortedMap<DD, R> invoke(ProxyFactory<ZZ, DD, T, E> map, Invoker<ZZ, DD, T, R, E> invoker) throws E
	{
		DatabaseCluster<ZZ, DD> cluster = map.getRoot().getDatabaseCluster();
		ExceptionFactory<E> exceptionFactory = map.getExceptionFactory();
		Balancer<ZZ, DD> balancer = cluster.getBalancer();
		Dialect dialect = cluster.getDialect();
		StateManager stateManager = cluster.getStateManager();
		
		@SuppressWarnings("unchecked")
		DD database = (DD) this.database;
		while (true)
		{	
			if (database == null)
			{
				throw exceptionFactory.createException(Messages.NO_ACTIVE_DATABASES.getMessage(cluster));	
			}
			
			T object = map.get(database);
			
			try
			{
				R result = balancer.invoke(invoker, database, object);
				
				SortedMap<DD, R> resultMap = new TreeMap<>();
				resultMap.put(database, result);
				return resultMap;
			}
			catch (Exception e)
			{
				E exception = exceptionFactory.createException(e);
				
				if (exceptionFactory.indicatesFailure(exception, dialect))
				{
					if (cluster.deactivate(database, stateManager))
					{
						logger.log(Level.ERROR, exception, Messages.DATABASE_DEACTIVATED.getMessage(), this.database, cluster);
					}
				}
				else
				{
					throw exception;
				}
			}
		}
	}
}
