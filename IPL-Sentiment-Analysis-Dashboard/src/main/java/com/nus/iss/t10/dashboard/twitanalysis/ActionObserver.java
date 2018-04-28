/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nus.iss.t10.dashboard.twitanalysis;

public interface ActionObserver {
    
    void setActionExecutor(ActionExecutor ae);
    
    void connect();
    
    void disconnect();
}
