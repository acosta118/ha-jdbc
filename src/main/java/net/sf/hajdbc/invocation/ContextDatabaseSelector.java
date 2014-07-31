package net.sf.hajdbc.invocation;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.balancer.Balancer;

/**
 * @author andre
 *
 */
public class ContextDatabaseSelector implements InvokeOnOneInvocationStrategy.DatabaseSelector
{

	@Override
	public <Z, D extends Database<Z>> D selectDatabase(Balancer<Z, D> balancer)
	{
		// TODO Auto-generated method stub
		return null;
	}

}
