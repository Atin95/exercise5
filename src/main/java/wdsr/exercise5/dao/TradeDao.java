package wdsr.exercise5.dao;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;
import wdsr.exercise5.model.Trade;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;

@Repository
public class TradeDao {

    @Autowired
    JdbcTemplate jdbcTemplate;

    /**
     * Zaimplementuj metode insertTrade aby wstawiała nowy rekord do tabeli "trade"
     * na podstawie przekazanego objektu klasy Trade.
     * @param trade
     * @return metoda powinna zwracać id nowego rekordu.
     */
    public int insertTrade(Trade trade) 
    {
    	PreparedStatementCreator psc = new PreparedStatementCreator() 
    	{
			@Override
			public PreparedStatement createPreparedStatement(Connection con) throws SQLException 
			{
				String query = "INSERT INTO trade (asset, amount, date) VALUES (?, ?, ?)";
				PreparedStatement ps = con.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
				ps.setString(1, trade.getAsset());
				ps.setDouble(2, trade.getAmount());
				ps.setDate(3, new java.sql.Date(trade.getDate().getTime()));
				return ps;
			}
		};
		KeyHolder kh = new GeneratedKeyHolder();
        jdbcTemplate.update(psc, kh);
        return kh.getKey().intValue();
    }

    /**
     * Zaimplementuj metode aby wyciągneła z bazy rekord o podanym id.
     * Użyj intrfejsu RowMapper.
     * @param id
     * @return metaoda powinna zwracać obiekt reprezentujący rekord o podanym id.
     */
    public Optional<Trade> extractTrade(int id) 
    {
        PreparedStatementCreator psc = new PreparedStatementCreator() 
        {
			@Override
			public PreparedStatement createPreparedStatement(Connection con) throws SQLException 
			{
				String query = "SELECT * FROM trade WHERE id = (?)";
				PreparedStatement ps = con.prepareStatement(query);
				ps.setInt(1, id);
				return ps;
			}
		};
		List<Trade> result = jdbcTemplate.query(psc, new RowMapper() 
		{
			@Override
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException 
			{
				Trade t = new Trade(rs.getInt(1), rs.getString(2), rs.getDouble(3), rs.getDate(4));
				return t;
			}
		});
		if (result.isEmpty())
		{
			return Optional.empty();
		}
		return Optional.of(result.get(0));
    }

    /**
     * Zaimplementuj metode aby wyciągneła z bazy rekord o podanym id.
     * @param id, rch - callback który przetworzy wyciągnięty wiersz.
     * @return metaoda powinna zwracać obiekt reprezentujący rekord o podanym id.
     */
    public void extractTrade(int id, RowCallbackHandler rch) 
    {
    	PreparedStatementCreator psc = new PreparedStatementCreator() 
        {
			@Override
			public PreparedStatement createPreparedStatement(Connection con) throws SQLException 
			{
				String query = "SELECT * FROM trade WHERE id = (?)";
				PreparedStatement ps = con.prepareStatement(query);
				ps.setInt(1, id);
				return ps;
			}
		};
		jdbcTemplate.query(psc, rch);
    }

    /**
     * Zaimplementuj metode aby zaktualizowała rekord o podanym id danymi z przekazanego parametru 'trade'
     * @param trade
     */
    public void updateTrade(int id, Trade trade) 
    {
        PreparedStatementCreator psc = new PreparedStatementCreator() 
        {
			@Override
			public PreparedStatement createPreparedStatement(Connection con) throws SQLException 
			{
				String query = "UPDATE trade SET asset = ?, amount = ?, date = ? WHERE id = ?";
				PreparedStatement ps = con.prepareStatement(query);
				ps.setString(1, trade.getAsset());
				ps.setDouble(2, trade.getAmount());
				ps.setDate(3, new java.sql.Date(trade.getDate().getTime()));
				ps.setInt(4, id);
				return ps;
			}
		};
		jdbcTemplate.update(psc);
    }

    /**
     * Zaimplementuj metode aby usuwała z bazy rekord o podanym id.
     * @param id
     */
    public void deleteTrade(int id) 
    {
        String query = "DELETE FROM trade WHERE id = ?";
        jdbcTemplate.update(query, id);
    }

}
