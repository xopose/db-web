package com.lazer.dbweb.controllers;

import com.lazer.dbweb.database.new_db.Database;
import com.lazer.dbweb.database.new_db.InMemoryDatabase;
import com.lazer.dbweb.database.new_db.exceptions.CantCreateDatabaseException;
import com.lazer.dbweb.database.new_db.exceptions.IncorrectCommandException;
import com.lazer.dbweb.database.new_db.exceptions.TableNotFoundException;
import com.lazer.dbweb.database.new_db.sql_processor.Starter;
import jakarta.annotation.PostConstruct;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DbResourceController {
    private Database database;
    private Starter starter;

    @PostConstruct
    public void init(){
        this.database = new InMemoryDatabase();
        this.starter = new Starter(this.database);
    }
    @PostMapping("/execute")
    //TODO переделать на тело
    public String index(String command) throws CantCreateDatabaseException, TableNotFoundException, IncorrectCommandException {
        return starter.execute(command).toString();
    }
}
