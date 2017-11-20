import React from 'react';
import {Link} from 'react-router';

export default function PureSolTechnologies() {
    return (
        <Link className="navbar-brand" to="/">
            <span className="logo"><span className="puresol">PureSol</span> <span className="technologies">Technologies</span></span>
        </Link>
    );
}
