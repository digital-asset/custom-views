package com.daml.quickstart.iou;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Component
public class EventsRepository {
  private final JdbcTemplate jdbcTemplate;

  @Autowired
  public EventsRepository(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  public List<ProjectedEvent> all() {
    return jdbcTemplate.query("SELECT * from events", new EventRowMapper());
  }

  public Integer count() {
    return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM events", Integer.class);
  }

  private static class EventRowMapper implements RowMapper<ProjectedEvent> {
    @Override
    public ProjectedEvent mapRow(ResultSet rs, int rowNum) throws SQLException {
      return new ProjectedEvent(
          rs.getString("event_id"),
          rs.getString("contract_id"),
          rs.getString("currency"),
          rs.getDouble("amount")
      );
    }
  }
}
