import React from 'react';
import { Link } from 'react-router';
import { HomeIcon } from 'react-octicons';

export default class Menu extends React.Component {

    constructor( props ) {
        super( props );
    }

    render() {
        return (
            <nav className="navbar navbar-expand-lg navbar-light bg-light">
                <Link className="navbar-brand" to="/">DuctileDB</Link>
                <button className="navbar-toggler" type="button" data-toggle="collapse" data-target="#navbarSupportedContent" aria-controls="navbarSupportedContent" aria-expanded="false" aria-label="Toggle navigation">
                    <span className="navbar-toggler-icon"></span>
                </button>

                <div className="collapse navbar-collapse" id="navbarSupportedContent">
                    <ul className="navbar-nav mr-auto">
                        <li className="nav-item active">
                            <Link className="nav-link" to="/"><HomeIcon /></Link>
                        </li>
                        <li className="nav-item">
                            <Link className="nav-link" to="/metrics">Metrics</Link>
                        </li>
                        <li className="nav-item">
                            <Link className="nav-link" to="/browser">Browser</Link>
                        </li>
                    </ul>
                    <form className="form-inline my-2 my-lg-0">
                        <input className="form-control mr-sm-2" type="text" placeholder="Search" aria-label="Search" />
                        <button className="btn btn-outline-success my-2 my-sm-0" type="submit">Search</button>
                    </form>
                </div>
            </nav>
        );
    }
}
