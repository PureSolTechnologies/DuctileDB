import { createStore, combineReducers } from 'redux';

function dummyReducer( state = null, action ) {
    if ( state === null ) {
        state = {};
    }
    return state;
}

const store = createStore( combineReducers(
    { dummy: dummyReducer } ) );

export default store;
