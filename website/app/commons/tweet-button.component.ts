import {Component} from 'angular2/core';

declare var twttr: any;

@Component({
	selector: 'tweet-button',
	template: `
<a href="https://twitter.com/share" class="twitter-share-button" data-related="puresoltech">Tweet</a>
<script>!function(d,s,id){var js,fjs=d.getElementsByTagName(s)[0],p=/^http:/.test(d.location)?'http':'https';if(!d.getElementById(id)){js=d.createElement(s);js.id=id;js.src=p+'://platform.twitter.com/widgets.js';fjs.parentNode.insertBefore(js,fjs);}}(document, 'script', 'twitter-wjs');</script>
`  
})
export class TweetButtonComponent {
  ngAfterContentInit(){
    twttr.widgets.load(document.getElementById("container"));
  }
}
